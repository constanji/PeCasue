const fs = require('fs').promises;
const path = require('path');
const ModelClient = require('./ModelClient');
const EvaluationService = require('./EvaluationService');

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

  static async runBenchmarkTask(taskId, config, benchmarkRoot, resultsDirOverride) {
    const root = benchmarkRoot || path.join(__dirname, '../../benchmark');
    const dataDir = path.join(root, 'data');
    const resultsDir = resultsDirOverride || path.join(root, 'results');
    const knowledgeDir = path.join(root, 'knowledge');

    const tasksMap = getTasks();
    const task = tasksMap.get(taskId);
    if (!task) throw new Error(`Task not found: ${taskId}`);

    BenchmarkService.knowledgeLoggedDbIds.clear();
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

      if (!config.endpointConfig.baseURL) {
        throw new Error('Endpoint baseURL is missing. Please check your endpoint configuration.');
      }
      if (!config.endpointConfig.apiKey) {
        throw new Error('Endpoint API key is missing. Please check your .env file or endpoint configuration.');
      }

      pushLog(`[BenchmarkService] 任务 ${taskId} 开始 | 端点: ${config.endpointConfig.name} | 模型: ${config.modelConfig.model} | 数据集: ${config.databaseName || '全部'} (${dataset.length} 题) | SQL方言: ${config.sqlDialect}`);
      const instruction = config.toolsConfig?.useKnowledge
        ? `知识库: 开, QA类型: ${config.toolsConfig?.qaType || '无'}`
        : '知识库: 关';
      pushLog(`[BenchmarkService] Instruction: ${instruction}`);

      const modelClient = new ModelClient(config.endpointConfig, config.modelConfig);
      const predictions = [];

      for (let i = 0; i < dataset.length; i++) {
        const item = dataset[i];
        const itemStartTime = Date.now();
        task.currentItem = item.question_id || `item_${i + 1}`;
        task.itemStartTime = new Date().toISOString();
        tasksMap.set(taskId, task);

        try {
          const predictedSQL = await this.generateSQL(modelClient, item, config.sqlDialect, config.toolsConfig, knowledgeDir);
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

          pushLog(`[BenchmarkService] 已完成 第 ${i + 1}/${dataset.length} 题 | question_id=${item.question_id} | 耗时 ${itemDuration}ms`);
          if ((i + 1) % 5 === 0 || i === dataset.length - 1) {
            const totalSoFar = Date.now() - task.startTime;
            pushLog(`[BenchmarkService] 进度 ${task.progress}% (${i + 1}/${dataset.length}) | 累计耗时 ${(totalSoFar / 1000).toFixed(1)}s`);
          }
        } catch (error) {
          const itemDuration = Date.now() - itemStartTime;
          pushLog(`[BenchmarkService] 第 ${i + 1}/${dataset.length} 题失败 question_id=${item.question_id}: ${error.message}`);
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
      const predictionsPath = await this.savePredictions(resultsDir, taskId, predictions, dataset);
      task.progress = 60;
      tasksMap.set(taskId, task);
      pushLog(`[BenchmarkService] 预测已保存，开始评估: ${config.evaluationMetrics.join(', ')}`);

      const evaluationResults = await EvaluationService.evaluate(
        root,
        predictionsPath,
        config.datasetId,
        config.sqlDialect,
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

  static async buildPrompt(item, sqlDialect, useKnowledge, knowledgeDir) {
    const schemaPrompt = await this.getSchemaPrompt(item.db_id, sqlDialect);
    const question = item.question;
    const evidence = item.evidence || '';

    let knowledgeContent = '';
    if (useKnowledge && knowledgeDir) {
      knowledgeContent = await this.loadKnowledgeFromDirectory(knowledgeDir, item.db_id);
      if (knowledgeContent && !BenchmarkService.knowledgeLoggedDbIds.has(item.db_id)) {
        BenchmarkService.knowledgeLoggedDbIds.add(item.db_id);
        console.log(`[BenchmarkService] 知识库已加载 db_id=${item.db_id} 长度=${knowledgeContent.length} 字符`);
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

    return `${schemaPrompt}${knowledgeSection}

-- Using valid ${sqlDialect}${evidence ? ' and understanding External Knowledge' : ''}${useKnowledge ? ' with Knowledge Base' : ''}, answer the following questions for the tables provided above.
-- ${question}
${evidence ? `-- External Knowledge: ${evidence}` : ''}

Generate the ${sqlDialect} SQL for the above question after thinking step by step:

In your response, you do not need to mention your intermediate steps.
Do not include any comments in your response.
Do not need to start with the symbol \`\`\`
You only need to return the result ${sqlDialect} SQL code
start from SELECT`;
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

  static async getSchemaPrompt(dbId, sqlDialect) {
    return `-- Database schema for ${dbId} (${sqlDialect})`;
  }

  static extractSQL(response) {
    let sql = response.trim();
    sql = sql.replace(/^```[\w]*\n?/g, '').replace(/\n?```$/g, '').trim();
    sql = sql.replace(/--.*$/gm, '');
    return sql.trim();
  }

  static async savePredictions(resultsDir, taskId, predictions, dataset) {
    await fs.mkdir(resultsDir, { recursive: true });

    const predictionsObj = {};
    predictions.forEach((pred) => {
      if (pred.sql) {
        predictionsObj[pred.index] = `${pred.sql}\t----- bird -----\t${pred.db_id}`;
      }
    });
    const filePath = path.join(resultsDir, `${taskId}_predictions.json`);
    await fs.writeFile(filePath, JSON.stringify(predictionsObj, null, 2));

    const groundTruthPath = path.join(resultsDir, `${taskId}_ground_truth.sql`);
    const groundTruthContent = dataset
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
