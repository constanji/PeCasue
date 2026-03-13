const fs = require('fs').promises;
const path = require('path');
const { execSync } = require('child_process');
const ModelClient = require('./ModelClient');
const EvaluationService = require('./EvaluationService');
const { isAgentsEndpoint, EModelEndpoint, Constants } = require('@because/data-provider');
const AgentClient = require('../controllers/agents/client');
const { initializeClient } = require('../services/Endpoints/agents/initialize');
const { getAppConfig } = require('../services/Config');
const { v4: uuidv4 } = require('uuid');

function getTasks() {
  const BenchmarkController = require('../controllers/BenchmarkController');
  const tasks = BenchmarkController.tasks;
  if (!tasks || typeof tasks.get !== 'function') {
    throw new Error('tasks is not a valid Map');
  }
  return tasks;
}

class BenchmarkService {
  static currentQAType = null;
  static knowledgeLoggedDbIds = new Set();
  static knowledgeCache = new Map(); // 缓存知识库内容，避免重复加载

  static async runBenchmarkTask(taskId, config, benchmarkRoot, resultsDirOverride) {
    const root = benchmarkRoot || path.join(__dirname, '../../benchmark');
    const dataDir = path.join(root, 'data');
    const resultsDir = resultsDirOverride || path.join(root, 'results');
    const knowledgeDir = path.join(root, 'knowledge');

    const tasksMap = getTasks();
    const task = tasksMap.get(taskId);
    if (!task) throw new Error(`Task not found: ${taskId}`);

    BenchmarkService.knowledgeLoggedDbIds.clear();
    BenchmarkService.knowledgeCache.clear();
    BenchmarkService.schemaCache.clear();
    if (!Array.isArray(task.statusLogs)) task.statusLogs = [];

    const pushLog = (msg) => {
      task.statusLogs.push(msg);
      console.log(msg);
      tasksMap.set(taskId, task);
    };

    try {
      task.status = 'running';
      task.progress = 0;
      task.startTime = Date.now();
      tasksMap.set(taskId, task);

      const dataset = await this.loadDataset(dataDir, config.datasetId, config.databaseName, config.sqlDialect);
      task.total = dataset.length;
      tasksMap.set(taskId, task);

      // 检查是否为 agents 端点
      const isAgents = isAgentsEndpoint(config.endpointConfig.name);

      // 对于 agents 端点，不需要 baseURL 和 apiKey
      if (!isAgents) {
        if (!config.endpointConfig.baseURL) {
          throw new Error('Endpoint baseURL is missing. Please check your endpoint configuration.');
        }
        if (!config.endpointConfig.apiKey) {
          throw new Error('Endpoint API key is missing. Please check your .env file or endpoint configuration.');
        }
      }
      const endpointType = isAgents ? '智能体 (Agents)' : '模型 (Model)';
      pushLog(`[BenchmarkService] 任务 ${taskId} 开始 | 端点: ${config.endpointConfig.name} (${endpointType}) | 模型: ${config.modelConfig.model} | 数据集: ${config.databaseName || '全部'} (${dataset.length} 题) | SQL方言: ${config.sqlDialect}`);
      const instruction = config.toolsConfig?.useKnowledge
        ? `知识库: 开, QA类型: ${config.toolsConfig?.qaType || '无'}`
        : '知识库: 关';
      pushLog(`[BenchmarkService] Instruction: ${instruction}`);

      let modelClient = null;
      let appConfig = null;
      let mockReq = null;
      let mockRes = null;

      // Agent 端点：只做一次 appConfig 加载和 mockReq/Res 创建；
      // agentClient 在每题循环内重新初始化，保证零上下文污染。
      if (isAgents) {
        appConfig = await getAppConfig({ role: 'USER' });
        // 禁用 Memory 和 QA Extractor，防止跨题记忆污染
        appConfig = {
          ...appConfig,
          memory: { ...(appConfig.memory || {}), disabled: true },
        };
        mockReq = this.createMockRequest(config, appConfig, config.userId);
        mockRes = this.createMockResponse();

        // 过滤 agent 工具：基准测试只保留 NL2SQL 相关工具
        const benchmarkAgent = this.createBenchmarkAgent(config.agentConfig.agent);
        config._benchmarkAgent = benchmarkAgent;
      } else {
        modelClient = new ModelClient(config.endpointConfig, config.modelConfig);
      }

      const QUESTION_TIMEOUT_MS = 120_000; // 单题超时 120 秒
      const predictions = [];

      for (let i = 0; i < dataset.length; i++) {
        const currentTask = tasksMap.get(taskId);
        if (currentTask?.cancelled) {
          pushLog(`[BenchmarkService] 任务已取消，停止处理`);
          task.status = 'cancelled';
          tasksMap.set(taskId, task);
          break;
        }

        const item = dataset[i];
        const itemStartTime = Date.now();
        task.currentItem = item.question_id || `item_${i + 1}`;
        task.itemStartTime = new Date().toISOString();
        tasksMap.set(taskId, task);

        try {
          let predictedSQL;

          if (isAgents) {
            // ★ 每题创建全新 AgentClient — 彻底隔离上下文、contentParts、memory
            // 必须深拷贝 agent：initializeAgent 会 mutate agent.provider / agent.endpoint
            const agentClone = structuredClone(config._benchmarkAgent);
            const { client: freshClient } = await initializeClient({
              req: mockReq,
              res: this.createMockResponse(),
              endpointOption: {
                endpoint: EModelEndpoint.agents,
                agent: Promise.resolve(agentClone),
                model_parameters: agentClone.model_parameters,
              },
            });
            freshClient.skipSaveConvo = true;
            freshClient.skipSaveUserMessage = true;
            freshClient.saveMessageToDatabase = async (message) => ({ message, conversation: null });

            // 带超时保护
            const timeoutPromise = new Promise((_, reject) =>
              setTimeout(() => reject(new Error(`单题超时 (>${QUESTION_TIMEOUT_MS / 1000}s)`)), QUESTION_TIMEOUT_MS),
            );
            predictedSQL = await Promise.race([
              this.generateSQLWithAgent(freshClient, item, config.sqlDialect, config.toolsConfig, knowledgeDir, mockReq),
              timeoutPromise,
            ]);
          } else {
            predictedSQL = await this.generateSQL(modelClient, item, config.sqlDialect, config.toolsConfig, knowledgeDir);
          }

          const itemDuration = Date.now() - itemStartTime;
          predictions.push({
            index: i,
            db_id: item.db_id,
            sql: predictedSQL,
            question_id: item.question_id,
            duration: itemDuration,
          });

          task.completed = i + 1;
          task.progress = Math.round(((i + 1) / dataset.length) * 50);
          task.lastCompletedItem = item.question_id || `item_${i + 1}`;
          task.lastItemDuration = itemDuration;
          task.currentItem = null;
          task.itemStartTime = null;
          tasksMap.set(taskId, task);

          pushLog(`[BenchmarkService] 已完成 第 ${i + 1}/${dataset.length} 题 | question_id=${item.question_id} | 耗时 ${(itemDuration / 1000).toFixed(1)}s`);
          if ((i + 1) % 5 === 0 || i === dataset.length - 1) {
            const totalSoFar = Date.now() - task.startTime;
            pushLog(`[BenchmarkService] 进度 ${task.progress}% (${i + 1}/${dataset.length}) | 累计耗时 ${(totalSoFar / 1000).toFixed(1)}s`);
          }
        } catch (error) {
          const itemDuration = Date.now() - itemStartTime;
          pushLog(`[BenchmarkService] 第 ${i + 1}/${dataset.length} 题失败 question_id=${item.question_id}: ${error.message} (${(itemDuration / 1000).toFixed(1)}s)`);
          predictions.push({ index: i, db_id: item.db_id, sql: null, error: error.message });
          task.completed = i + 1;
          task.progress = Math.round(((i + 1) / dataset.length) * 50);
          task.lastCompletedItem = item.question_id || `item_${i + 1}`;
          task.currentItem = null;
          task.itemStartTime = null;
          tasksMap.set(taskId, task);
        }
      }

      pushLog(`[BenchmarkService] 保存预测结果...`);
      const predictionsPath = await this.savePredictions(resultsDir, taskId, predictions, dataset, config.sqlDialect);
      task.progress = 60;
      tasksMap.set(taskId, task);

      // EX 评估总是在 SQLite 上执行（因为只有 .sqlite 数据库文件）
      const evalDialect = 'SQLite';
      if (config.sqlDialect.toLowerCase() !== 'sqlite') {
        pushLog(`[BenchmarkService] 注意: 用户选择 ${config.sqlDialect} 方言，EX评估将使用 SQLite 执行（已自动加载 SQLite ground truth）`);
      }
      pushLog(`[BenchmarkService] 预测已保存，开始评估: ${config.evaluationMetrics.join(', ')}`);

      const evaluationResults = await EvaluationService.evaluate(
        root,
        predictionsPath,
        config.datasetId,
        evalDialect,
        config.evaluationMetrics,
        (progress) => {
          const currentTask = tasksMap.get(taskId);
          if (currentTask) {
            currentTask.progress = 60 + Math.round(progress * 0.4);
            tasksMap.set(taskId, currentTask);
          }
        },
      );

      task.status = 'completed';
      task.progress = 100;
      task.results = evaluationResults;
      task.completedAt = new Date().toISOString();
      tasksMap.set(taskId, task);

      const resultsFilePath = path.join(resultsDir, `${taskId}_results.json`);
      pushLog(`[BenchmarkService] 任务 ${taskId} 完成，结果已写入 ${resultsFilePath}`);
      await fs.writeFile(resultsFilePath, JSON.stringify(evaluationResults, null, 2));
    } catch (error) {
      const currentTask = tasksMap.get(taskId);
      if (currentTask) {
        currentTask.status = 'failed';
        currentTask.error = error.message;
        tasksMap.set(taskId, currentTask);
      }
      throw error;
    }
  }

  static async loadDataset(dataDir, datasetId, databaseName, sqlDialect) {
    let fileName = datasetId;
    if (!datasetId.includes('mini_dev')) {
      fileName = `mini_dev_${sqlDialect.toLowerCase()}.json`;
    }
    const filePath = path.join(dataDir, fileName);
    try {
      const content = await fs.readFile(filePath, 'utf-8');
      let dataset = JSON.parse(content);
      if (databaseName) {
        dataset = dataset.filter((item) => item.db_id === databaseName);
        if (dataset.length === 0) {
          throw new Error(`No data found for database: ${databaseName} in ${fileName}`);
        }
      }
      return dataset;
    } catch (error) {
      if (error.code === 'ENOENT') {
        throw new Error(`Dataset file not found: ${filePath}. Please ensure the dataset file exists.`);
      }
      throw new Error(`Failed to load dataset: ${error.message}`);
    }
  }

  static async generateSQL(modelClient, item, sqlDialect, toolsConfig, knowledgeDir) {
    const useKnowledge = toolsConfig?.useKnowledge || false;
    const qaType = toolsConfig?.qaType || null;
    BenchmarkService.currentQAType = qaType;

    const prompt = await this.buildPrompt(item, sqlDialect, useKnowledge, knowledgeDir);
    const response = await modelClient.generate(prompt, { temperature: 0.1, max_tokens: 2000 });
    return this.extractSQL(response);
  }

  static async generateSQLWithAgent(agentClient, item, sqlDialect, toolsConfig, knowledgeDir, req) {
    const useKnowledge = toolsConfig?.useKnowledge || false;
    const qaType = toolsConfig?.qaType || null;
    BenchmarkService.currentQAType = qaType;

    const prompt = await this.buildAgentPrompt(item, sqlDialect, useKnowledge, knowledgeDir);

    const conversationId = uuidv4();
    const responseMessageId = uuidv4();

    // 更新 mockReq.body.conversationId，确保工具在此次调用中能使用正确的会话上下文
    if (req && req.body) {
      req.body.conversationId = conversationId;
    }

    const messageOptions = {
      user: req.user?.id,
      conversationId,
      parentMessageId: Constants.NO_PARENT,
      responseMessageId,
      abortController: new AbortController(),
      progressOptions: {
        res: null,
      },
    };

    const response = await agentClient.sendMessage(prompt, messageOptions);
    
    const responseText = this.extractTextFromAgentResponse(response);
    const extractedSQL = this.extractSQLFromAgentResponse(responseText);

    const qid = item.question_id || 'unknown';
    const preview = (extractedSQL || '').substring(0, 120).replace(/\n/g, ' ');
    console.log(`[BenchmarkService] Agent SQL 提取 question_id=${qid} | 原始长度=${responseText.length} | SQL预览: ${preview}`);

    if (!extractedSQL || !/\b(SELECT|WITH)\b/i.test(extractedSQL)) {
      console.warn(`[BenchmarkService] 未能从 Agent 响应中提取有效 SQL question_id=${qid} | 原始响应前500字: ${responseText.substring(0, 500)}`);
    }

    return extractedSQL;
  }

  static extractTextFromAgentResponse(response) {
    if (response.content && typeof response.content === 'string') {
      return response.content;
    }
    if (Array.isArray(response.content)) {
      const textParts = response.content
        .filter((part) => {
          if (typeof part === 'string') return true;
          return part.type === 'text' || part.type === 'text_delta';
        })
        .map((part) => {
          if (typeof part === 'string') return part;
          return part.text || part.value || '';
        })
        .filter(Boolean);
      if (textParts.length > 0) {
        return textParts.join('\n');
      }
    }
    if (response.text) {
      return response.text;
    }
    return JSON.stringify(response);
  }

  static createMockRequest(config, appConfig, userId) {
    if (!userId) {
      throw new Error('用户ID是必需的，请确保在配置中传递 userId');
    }
    return {
      user: {
        id: userId,
        role: 'USER',
      },
      config: appConfig || {
        endpoints: {
          [EModelEndpoint.agents]: {},
        },
      },
      body: {
        conversationId: config._benchmarkConversationId || null,
        data_source_id: config.toolsConfig?.dataSourceId || null,
        project_id: config.toolsConfig?.projectId || null,
        _benchmarkAllowedCommands: ['database-schema', 'sql-validation'],
      },
    };
  }

  static createMockResponse() {
    return {
      on: () => {},
      write: () => {},
      end: () => {},
      status: () => ({ json: () => {} }),
      json: () => {},
      clearCookie: () => {},
      setHeader: () => {},
      getHeader: () => undefined,
    };
  }

  static async buildPrompt(item, sqlDialect, useKnowledge, knowledgeDir) {
    const schemaPrompt = await this.getSchemaPrompt(item.db_id, sqlDialect);
    const question = item.question;
    const evidence = item.evidence || '';

    let knowledgeContent = '';
    if (useKnowledge && knowledgeDir) {
      // 使用缓存避免重复加载
      const cacheKey = `${item.db_id}_${BenchmarkService.currentQAType || 'none'}`;
      if (BenchmarkService.knowledgeCache.has(cacheKey)) {
        knowledgeContent = BenchmarkService.knowledgeCache.get(cacheKey);
      } else {
        knowledgeContent = await this.loadKnowledgeFromDirectory(knowledgeDir, item.db_id);
        if (knowledgeContent) {
          BenchmarkService.knowledgeCache.set(cacheKey, knowledgeContent);
          if (!BenchmarkService.knowledgeLoggedDbIds.has(item.db_id)) {
            BenchmarkService.knowledgeLoggedDbIds.add(item.db_id);
            console.log(`[BenchmarkService] 知识库已加载 db_id=${item.db_id} 长度=${knowledgeContent.length} 字符`);
          }
        }
      }
    }

    let knowledgeSection = '';
    if (knowledgeContent) {
      const maxKnowledgeLength = 8000;
      const truncatedKnowledge =
        knowledgeContent.length > maxKnowledgeLength
          ? knowledgeContent.substring(0, maxKnowledgeLength) + '\n\n...(知识库内容已截断)'
          : knowledgeContent;
      knowledgeSection = `\n# Knowledge Base:\n${truncatedKnowledge}\n`;
    }

    const dialectGuide = this.getDialectGuide();

    return `${schemaPrompt}${knowledgeSection}

-- SQLite dialect rules: ${dialectGuide}

-- Using valid SQLite SQL${evidence ? ' and understanding External Knowledge' : ''}${useKnowledge ? ' with Knowledge Base' : ''}, answer the following questions for the tables provided above.
-- ${question}
${evidence ? `-- External Knowledge: ${evidence}` : ''}

Generate the SQLite SQL for the above question after thinking step by step:

In your response, you do not need to mention your intermediate steps.
Do not include any comments in your response.
Do not need to start with the symbol \`\`\`
You only need to return the result SQLite SQL code
start from SELECT`;
  }

  static async buildAgentPrompt(item, sqlDialect, useKnowledge, knowledgeDir) {
    // 使用 Python 脚本生成智能体提示词
    const { spawn } = require('child_process');
    const { promisify } = require('util');
    const schemaPrompt = await this.getSchemaPrompt(item.db_id, sqlDialect);
    const question = item.question;
    const evidence = item.evidence || '';

    let knowledgeContent = '';
    if (useKnowledge && knowledgeDir) {
      // 使用缓存避免重复加载
      const cacheKey = `${item.db_id}_${BenchmarkService.currentQAType || 'none'}`;
      if (BenchmarkService.knowledgeCache.has(cacheKey)) {
        knowledgeContent = BenchmarkService.knowledgeCache.get(cacheKey);
      } else {
        knowledgeContent = await this.loadKnowledgeFromDirectory(knowledgeDir, item.db_id);
        if (knowledgeContent) {
          BenchmarkService.knowledgeCache.set(cacheKey, knowledgeContent);
          if (!BenchmarkService.knowledgeLoggedDbIds.has(item.db_id)) {
            BenchmarkService.knowledgeLoggedDbIds.add(item.db_id);
            console.log(`[BenchmarkService] 知识库已加载 db_id=${item.db_id} 长度=${knowledgeContent.length} 字符`);
          }
        }
      }
    }

    // 构建智能体提示词（使用中文，更适合智能体理解）
    let knowledgeSection = '';
    if (knowledgeContent) {
      const maxKnowledgeLength = 8000;
      const truncatedKnowledge =
        knowledgeContent.length > maxKnowledgeLength
          ? knowledgeContent.substring(0, maxKnowledgeLength) + '\n\n...(知识库内容已截断)'
          : knowledgeContent;
      knowledgeSection = `\n# 知识库:\n${truncatedKnowledge}\n`;
    }

    const dialectGuide = this.getDialectGuide();

    return `# 数据库表结构
${schemaPrompt}
${knowledgeSection}
# 任务

根据上方的数据库表结构，为以下问题生成正确的 **SQLite** SQL 查询。

**问题**: ${question}
${evidence ? `**外部知识**: ${evidence}` : ''}

# SQL 方言要求（SQLite）
${dialectGuide}

# Join Graph（表关联最短路径）

\`\`\`
district ─── client        （client.district_id = district.district_id）
district ─── account       （account.district_id = district.district_id）
client ───── disp ───── account  （disp 是 client↔account 的桥表）
account ──── loan          （loan.account_id = account.account_id）
account ──── trans         （trans.account_id = account.account_id）
account ──── order         （order.account_id = account.account_id）
disp ─────── card          （card.disp_id = disp.disp_id）
\`\`\`

⚠️ **JOIN 规则**：
- client + district 属性 → \`client JOIN district\`，**不要** 经过 disp/account
- account + district 属性 → \`account JOIN district\`，**不要** 经过 client/disp
- client + account 属性 → **必须经过 disp 桥表**
- 只 JOIN 问题**真正需要**的表，不要多余 JOIN

# 枚举值映射（自然语言 → 数据库实际值）

数据库使用捷克语枚举值，查询时**必须使用实际值**，不能用英文：
- credit card withdrawal → \`'VYBER KARTOU'\`
- cash withdrawal → \`'VYBER'\`
- cash deposit → \`'VKLAD'\`
- collection from another bank → \`'PREVOD Z UCTU'\`
- remittance to another bank → \`'PREVOD NA UCET'\`
- insurance payment → \`'POJISTNE'\`
- household payment → \`'SIPO'\`
- loan payment → \`'UVER'\`
- monthly issuance → \`'POPLATEK MESICNE'\`
- permanent order（定期转账指令） → 查 \`order\` 表
- transaction（实际交易） → 查 \`trans\` 表

# SQL 生成规则

1. **只返回问题要求的字段** — 不要附带额外列
2. **使用最短 JOIN 路径** — 参考上方 Join Graph
3. **优先简洁写法** — 找 MAX/MIN 用 \`ORDER BY ... LIMIT 1\`，不用子查询
4. **避免不必要的 CTE** — 除非确实需要多步聚合
5. **使用实际枚举值** — 参考上方映射表
6. **不要执行 SQL** — 只需要生成

# Think step by step

**Step 1 — Identify required tables**
从问题中提取关键实体，确定需要查询哪些表。只选择**真正需要**的表。

**Step 2 — Identify join keys**
根据 Join Graph 确定表之间的关联键。选择**最短路径**，不要多余 JOIN。

**Step 3 — Identify filter conditions**
确定 WHERE / HAVING / GROUP BY / ORDER BY / LIMIT 和聚合函数（COUNT/SUM/AVG/MAX/MIN）。
注意使用数据库中的**实际列名和枚举值**。

**Step 4 — Construct SQL**
基于以上分析，构造简洁、正确的 SQLite SQL。

# 可用工具

- **database-schema**：获取表结构详情。调用：\`{"command": "database-schema", "arguments": "{\\"format\\":\\"detailed\\"}"}\`
- **sql-validation**：校验 SQL 语法。调用：\`{"command": "sql-validation", "arguments": "{\\"sql\\":\\"SELECT ...\\"}"}\`

【输出格式】最终回复中必须包含一个 \`\`\`sql 代码块：
\`\`\`sql
SELECT ...
\`\`\``;
  }

  static async loadKnowledgeFromDirectory(dir, dbId) {
    try {
      await fs.access(dir);
    } catch {
      return '';
    }

    const knowledgeParts = [];

    const schemaDir = path.join(dir, 'schema');
    try {
      const schemaFiles = await fs.readdir(schemaDir);
      const yamlFiles = schemaFiles.filter(
        (f) =>
          (f.endsWith('.yaml') || f.endsWith('.yml')) &&
          (f.toLowerCase().includes(dbId.toLowerCase()) || f === 'Semantic Schema.yaml'),
      );
      for (const yamlFile of yamlFiles) {
        const yamlPath = path.join(schemaDir, yamlFile);
        const yamlContent = await this.parseYAMLFile(yamlPath);
        if (yamlContent) knowledgeParts.push(`# Semantic Schema (${yamlFile}):\n${yamlContent}`);
      }
    } catch {
      // ignore
    }

    const knowledgeSubDir = path.join(dir, 'knowledge');
    try {
      const knowledgeFiles = await fs.readdir(knowledgeSubDir);
      const mdFiles = knowledgeFiles.filter(
        (f) => f.endsWith('.md') && (f.includes(dbId) || f.toLowerCase().includes(dbId.toLowerCase())),
      );
      for (const mdFile of mdFiles) {
        const content = await fs.readFile(path.join(knowledgeSubDir, mdFile), 'utf-8');
        knowledgeParts.push(`# Knowledge Document (${mdFile}):\n${content}`);
      }
    } catch {
      // ignore
    }

    const qaType = BenchmarkService.currentQAType;
    const qaDir = path.join(dir, 'qa_pairs');
    if (qaType) {
      try {
        const qaFiles = await fs.readdir(qaDir);
        const txtFiles = qaFiles.filter((f) => {
          if (!f.endsWith('.txt')) return false;
          const matchesDb = f.includes(dbId) || f.toLowerCase().includes(dbId.toLowerCase());
          return matchesDb && new RegExp(`${dbId}_qa_${qaType}\\.txt$`, 'i').test(f);
        });
        for (const txtFile of txtFiles) {
          const content = await fs.readFile(path.join(qaDir, txtFile), 'utf-8');
          const typeName =
            qaType === 'desc' ? '问题和Description' : qaType === 'sql' ? '问题和SQL示例' : '问题+Description+SQL示例';
          knowledgeParts.push(`# QA Pairs (${typeName}):\n${content}`);
        }
      } catch {
        // ignore
      }
    }

    return knowledgeParts.length > 0 ? knowledgeParts.join('\n\n---\n\n') : '';
  }

  static async parseYAMLFile(yamlPath) {
    try {
      const yaml = require('js-yaml');
      const content = await fs.readFile(yamlPath, 'utf-8');
      const data = yaml.load(content);
      return this.formatYAMLForLLM(data);
    } catch {
      try {
        return await fs.readFile(yamlPath, 'utf-8');
      } catch {
        return '';
      }
    }
  }

  static formatYAMLForLLM(data) {
    let result = '';
    if (data.database) {
      result += `## Database: ${data.database.name || 'Unknown'}\n`;
      if (data.database.description) result += `${data.database.description}\n\n`;
    }
    if (data.entities) {
      result += '## Entities (Tables):\n\n';
      for (const [entityName, entityInfo] of Object.entries(data.entities)) {
        result += `### ${entityName}\n`;
        if (entityInfo.description) result += `**Description**: ${entityInfo.description}\n`;
        if (entityInfo.primary_key) result += `**Primary Key**: ${entityInfo.primary_key}\n`;
        if (entityInfo.fields) {
          result += '**Fields**:\n';
          for (const [fieldName, fieldDesc] of Object.entries(entityInfo.fields)) {
            result += `  - ${fieldName}: ${fieldDesc}\n`;
          }
        }
        result += '\n';
      }
    }
    if (data.relationships && Array.isArray(data.relationships)) {
      result += '## Relationships:\n';
      data.relationships.forEach((rel) => {
        result += `- ${rel}\n`;
      });
    }
    return result;
  }

  /**
   * 从 agent 配置中过滤工具，只保留基准测试需要的子命令。
   * rag-retrieval / intent-classification / chart-generation 等在 NL2SQL 场景无用，
   * 且 rag-retrieval 经常被模型以空参数调用导致报错和额外耗时。
   */
  static createBenchmarkAgent(agent) {
    const BENCHMARK_ALLOWED_TOOLS = new Set([
      'because_skills',
      'database_schema',
      'sql_executor',
      'sql_validation',
    ]);

    const filteredTools = (agent.tools || []).filter((t) => {
      const toolName = typeof t === 'string' ? t : t?.name || t?.type;
      return BENCHMARK_ALLOWED_TOOLS.has(toolName);
    });

    return {
      ...agent,
      tools: filteredTools.length > 0 ? filteredTools : agent.tools,
    };
  }

  static getDialectGuide() {
    return `- 使用 SQLite 语法，**不要**使用 MySQL/PostgreSQL 特有函数
- 日期提取: 使用 STRFTIME('%Y', date_col) 而非 DATE_FORMAT() 或 YEAR()
- 类型转换: 使用 CAST(x AS REAL) 而非 CAST(x AS DOUBLE)
- 字符串: 使用单引号 'value'，表名/列名**不要**加反引号
- 布尔判断: SUM(col = 'value') 直接使用，SQLite 中比较返回 0/1
- 日期比较: 直接用字符串比较 date_col < '1950-01-01'
- 不支持: DATE_FORMAT, YEAR(), MONTH(), TIMESTAMPDIFF, CURDATE()
- 可用: STRFTIME, DATE, TIME, JULIANDAY, || (字符串拼接)`;
  }

  static schemaCache = new Map();

  static async getSchemaPrompt(dbId, sqlDialect) {
    const cacheKey = `${dbId}_${sqlDialect}`;
    if (BenchmarkService.schemaCache.has(cacheKey)) {
      return BenchmarkService.schemaCache.get(cacheKey);
    }

    const benchmarkRoot = path.join(__dirname, '../../benchmark');

    // 1. 尝试从 SQLite 文件读取真实 DDL
    const sqlitePath = path.join(benchmarkRoot, 'data', 'dev_databases', dbId, `${dbId}.sqlite`);
    try {
      await fs.access(sqlitePath);
      const schema = this.loadSchemaFromSQLite(sqlitePath, dbId);
      if (schema) {
        BenchmarkService.schemaCache.set(cacheKey, schema);
        console.log(`[BenchmarkService] 从 SQLite 文件加载 schema: ${dbId} (${schema.length} 字符)`);
        return schema;
      }
    } catch {
      // SQLite 文件不存在，继续尝试其他方式
    }

    // 2. 尝试从 dev_tables.json 构建 schema
    const devTablesPath = path.join(benchmarkRoot, 'data', 'dev_tables.json');
    try {
      const content = await fs.readFile(devTablesPath, 'utf-8');
      const allTables = JSON.parse(content);
      const dbInfo = allTables.find((db) => db.db_id === dbId);
      if (dbInfo) {
        const schema = this.buildSchemaFromDevTables(dbInfo, sqlDialect);
        BenchmarkService.schemaCache.set(cacheKey, schema);
        console.log(`[BenchmarkService] 从 dev_tables.json 加载 schema: ${dbId} (${schema.length} 字符)`);
        return schema;
      }
    } catch {
      // dev_tables.json 不存在
    }

    // 3. 兜底占位符
    const fallback = `-- Database schema for ${dbId} (${sqlDialect})`;
    console.warn(`[BenchmarkService] 未能加载 schema，使用占位符: ${dbId}`);
    return fallback;
  }

  static loadSchemaFromSQLite(sqlitePath, dbId) {
    try {
      const output = execSync(`sqlite3 "${sqlitePath}" ".schema"`, {
        encoding: 'utf-8',
        timeout: 10000,
      });
      if (!output || !output.trim()) return null;

      const lines = output.split('\n').filter((line) => {
        const trimmed = line.trim();
        return trimmed && !trimmed.startsWith('CREATE INDEX') && !trimmed.startsWith('CREATE UNIQUE INDEX');
      });
      return `-- Schema for database: ${dbId}\n${lines.join('\n')}`;
    } catch (err) {
      console.warn(`[BenchmarkService] sqlite3 CLI 加载 schema 失败: ${err.message}`);
      return null;
    }
  }

  static buildSchemaFromDevTables(dbInfo, sqlDialect) {
    const tables = dbInfo.table_names_original || [];
    const columns = dbInfo.column_names_original || [];
    const types = dbInfo.column_types || [];
    const primaryKeys = new Set(dbInfo.primary_keys || []);
    const foreignKeys = dbInfo.foreign_keys || [];

    const ddlParts = [];

    for (let tIdx = 0; tIdx < tables.length; tIdx++) {
      const tableName = tables[tIdx];
      const tableCols = [];

      for (let cIdx = 0; cIdx < columns.length; cIdx++) {
        const [tableIndex, colName] = columns[cIdx];
        if (tableIndex !== tIdx) continue;

        const colType = types[cIdx] || 'TEXT';
        const isPK = primaryKeys.has(cIdx);
        tableCols.push(
          `  \`${colName}\` ${colType}${isPK ? ' PRIMARY KEY' : ''}`,
        );
      }

      const fkLines = foreignKeys
        .filter(([fromIdx]) => {
          const [tblIdx] = columns[fromIdx] || [];
          return tblIdx === tIdx;
        })
        .map(([fromIdx, toIdx]) => {
          const [, fromCol] = columns[fromIdx] || [];
          const [toTblIdx, toCol] = columns[toIdx] || [];
          const toTable = tables[toTblIdx];
          return `  FOREIGN KEY (\`${fromCol}\`) REFERENCES \`${toTable}\`(\`${toCol}\`)`;
        });

      const allLines = [...tableCols, ...fkLines];
      ddlParts.push(`CREATE TABLE \`${tableName}\` (\n${allLines.join(',\n')}\n);`);
    }

    return `-- Schema for database: ${dbInfo.db_id} (${sqlDialect})\n${ddlParts.join('\n\n')}`;
  }

  static extractSQL(response) {
    let sql = response.trim();
    sql = sql.replace(/^```[\w]*\n?/g, '').replace(/\n?```$/g, '').trim();
    sql = sql.replace(/--.*$/gm, '');
    return sql.trim();
  }

  /**
   * 从 Agent 的复杂响应中提取纯 SQL 语句。
   * 策略（按优先级）：
   *   1. 最后一个 ```sql ... ``` 代码块中的内容
   *   2. 最后一个通用代码块 ``` ... ``` 中包含 SELECT/WITH 的内容
   *   3. 逐行扫描，拼接最后一段以 SELECT/WITH 开头的连续 SQL 语句
   */
  static extractSQLFromAgentResponse(text) {
    if (!text || typeof text !== 'string') return null;

    // 1. 提取所有 ```sql ... ``` 代码块
    const sqlCodeBlockRegex = /```(?:sql|SQL)\s*\n([\s\S]*?)```/g;
    const sqlBlocks = [];
    let m;
    while ((m = sqlCodeBlockRegex.exec(text)) !== null) {
      const content = m[1].trim();
      if (content) sqlBlocks.push(content);
    }
    if (sqlBlocks.length > 0) {
      return this.cleanExtractedSQL(sqlBlocks[sqlBlocks.length - 1]);
    }

    // 2. 提取所有通用代码块，找包含 SELECT/WITH 的
    const genericCodeBlockRegex = /```\w*\s*\n([\s\S]*?)```/g;
    const candidateBlocks = [];
    while ((m = genericCodeBlockRegex.exec(text)) !== null) {
      const content = m[1].trim();
      if (content && /\b(SELECT|WITH)\b/i.test(content)) {
        candidateBlocks.push(content);
      }
    }
    if (candidateBlocks.length > 0) {
      return this.cleanExtractedSQL(candidateBlocks[candidateBlocks.length - 1]);
    }

    // 3. 逐行扫描，找最后一段以 SELECT/WITH 起头的连续 SQL
    const lines = text.split('\n');
    let lastSQLStart = -1;
    let lastSQLEnd = -1;
    let inSQL = false;

    for (let i = 0; i < lines.length; i++) {
      const trimmed = lines[i].trim();
      if (!inSQL && /^(SELECT|WITH)\b/i.test(trimmed)) {
        inSQL = true;
        lastSQLStart = i;
        lastSQLEnd = i;
      } else if (inSQL) {
        if (this.looksLikeSQLContinuation(trimmed)) {
          lastSQLEnd = i;
        } else {
          inSQL = false;
        }
      }
    }

    if (lastSQLStart !== -1) {
      const sqlLines = lines.slice(lastSQLStart, lastSQLEnd + 1).join('\n');
      return this.cleanExtractedSQL(sqlLines);
    }

    // 4. 兜底：整段文本当作 SQL 尝试清理
    return this.extractSQL(text);
  }

  static looksLikeSQLContinuation(line) {
    if (!line || line === '') return false;
    // 排除明显的自然语言行
    if (/^(这|以上|注意|说明|解释|其中|该|请|上述|结果|最终)/u.test(line)) return false;
    if (/^#{1,6}\s/.test(line)) return false;
    if (/^\*\*/.test(line)) return false;
    if (/^[-•]\s/.test(line)) return false;
    // SQL 关键词或延续特征
    const sqlPatterns = /^(FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|CROSS|ON|AND|OR|GROUP|ORDER|HAVING|LIMIT|OFFSET|UNION|INTERSECT|EXCEPT|INSERT|UPDATE|DELETE|SET|VALUES|INTO|AS|CASE|WHEN|THEN|ELSE|END|NOT|IN|EXISTS|BETWEEN|LIKE|IS|NULL|ASC|DESC|DISTINCT|ALL|ANY|SOME|CREATE|ALTER|DROP|TRUNCATE|WITH|RECURSIVE|OVER|PARTITION|WINDOW|FETCH|LATERAL|NATURAL|USING|FULL|SEMI|ANTI|QUALIFY|\(|,|\))/i;
    return sqlPatterns.test(line) || /;\s*$/.test(line);
  }

  static cleanExtractedSQL(sql) {
    if (!sql) return sql;
    let cleaned = sql.trim();
    cleaned = cleaned.replace(/^```[\w]*\n?/g, '').replace(/\n?```$/g, '').trim();
    cleaned = cleaned.replace(/--.*$/gm, '').trim();
    cleaned = cleaned.replace(/;\s*$/, '').trim();
    return cleaned;
  }

  static async savePredictions(resultsDir, taskId, predictions, dataset, sqlDialect) {
    await fs.mkdir(resultsDir, { recursive: true });

    const predictionsObj = {};
    predictions.forEach((pred) => {
      const sql = pred.sql || 'SELECT 1';
      predictionsObj[pred.index] = `${sql}\t----- bird -----\t${pred.db_id}`;
    });
    const filePath = path.join(resultsDir, `${taskId}_predictions.json`);
    await fs.writeFile(filePath, JSON.stringify(predictionsObj, null, 2));

    // EX 评估只能在 SQLite 上执行（我们只有 .sqlite 数据库文件）。
    // 当用户选择了 MySQL/PostgreSQL 方言时，自动加载 SQLite 版 ground truth 用于评估，
    // 确保 ground truth SQL 能在 SQLite 上正确执行。
    let evalDataset = dataset;
    if (sqlDialect && sqlDialect.toLowerCase() !== 'sqlite') {
      const benchmarkRoot = path.join(__dirname, '../../benchmark');
      const sqliteDatasetPath = path.join(benchmarkRoot, 'data', 'mini_dev_sqlite.json');
      try {
        const sqliteContent = await fs.readFile(sqliteDatasetPath, 'utf-8');
        const sqliteDataset = JSON.parse(sqliteContent);
        const questionIdMap = new Map();
        sqliteDataset.forEach((item) => questionIdMap.set(`${item.question_id}_${item.db_id}`, item));

        evalDataset = dataset.map((item) => {
          const key = `${item.question_id}_${item.db_id}`;
          const sqliteItem = questionIdMap.get(key);
          if (sqliteItem) {
            return { ...item, SQL: sqliteItem.SQL };
          }
          return item;
        });
        console.log(`[BenchmarkService] EX评估: 方言为 ${sqlDialect}，已加载 SQLite ground truth 替代 (匹配 ${evalDataset.filter((_, i) => {
          const key = `${dataset[i].question_id}_${dataset[i].db_id}`;
          return questionIdMap.has(key);
        }).length}/${dataset.length} 题)`);
      } catch (err) {
        console.warn(`[BenchmarkService] 无法加载 SQLite ground truth: ${err.message}，使用原始方言`);
      }
    }

    const groundTruthPath = path.join(resultsDir, `${taskId}_ground_truth.sql`);
    const groundTruthContent = evalDataset
      .map((item) => {
        let sql = (item.SQL || '').trim().replace(/\s+/g, ' ');
        return `${sql}\t${item.db_id}`;
      })
      .join('\n');
    await fs.writeFile(groundTruthPath, groundTruthContent);

    const diffJsonlPath = path.join(resultsDir, `${taskId}_diff.jsonl`);
    const diffJsonlContent =
      dataset
        .map((item) =>
          JSON.stringify({
            question_id: item.question_id,
            db_id: item.db_id,
            difficulty: item.difficulty || 'simple',
          }),
        )
        .join('\n') + '\n';
    await fs.writeFile(diffJsonlPath, diffJsonlContent);

    return { predictions: filePath, groundTruth: groundTruthPath, diffJsonl: diffJsonlPath };
  }
}

module.exports = BenchmarkService;
