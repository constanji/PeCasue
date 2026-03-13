const path = require('path');
const crypto = require('crypto');
const { getCustomEndpointConfig, isUserProvided } = require('@because/api');
const { extractEnvVariable, envVarRegex, EModelEndpoint, isAgentsEndpoint } = require('@because/data-provider');
const { getAppConfig } = require('../services/Config');
const { getUserKeyValues } = require('../services/UserService');
const BenchmarkService = require('../services/BenchmarkService');
const { getAgent } = require('~/models/Agent');

const getBenchmarkRoot = () => path.join(__dirname, '../../benchmark');
// 结果目录放在项目根下，避免 nodemon 监听 api 时因写入结果文件而重启
const getResultsDir = () => path.join(process.cwd(), 'benchmark_results');

/** 总耗时：已完成任务用完成时间减开始时间（固定），未完成用当前时间减开始时间 */
function getTotalDuration(task) {
  if (!task || !task.startTime) return null;
  if (task.status === 'completed' && task.completedAt) {
    return new Date(task.completedAt).getTime() - task.startTime;
  }
  return Date.now() - task.startTime;
}

const tasks = new Map();

class BenchmarkController {
  static async runBenchmark(req, res) {
    try {
      const {
        datasetId,
        sqlDialect,
        databaseName,
        endpointName,
        model,
        toolsConfig,
        evaluationMetrics = ['EX'],
        dataSourceId,
      } = req.body;

      if (!sqlDialect || !endpointName || !model) {
        return res.status(400).json({
          error: 'Missing required parameters: sqlDialect, endpointName, model',
        });
      }

      // 基准测试当前仅支持 OpenAI 兼容（custom）端点；Agents 端点需要走内部 Agent 运行链路
      if (false && String(endpointName).toLowerCase() === 'agents') {
        return res.status(400).json({
          error:
            '现有基准测试链路只支持“OpenAI 兼容”的 LLM 端点，直接调用模型生成 SQL；Agents 端点走的是内部智能体链路（有自己的聊天/工具/记忆流程），接口和鉴权都不同，需要改动较大',
        });
      }

      const appConfig = await getAppConfig({ role: req.user?.role });
      const isAgents = isAgentsEndpoint(endpointName);

      let resolvedEndpointConfig;
      let modelConfig;
      let agentConfig = null;

      if (isAgents) {
        // 智能体端点：model 参数实际上是 agent_id
        const agentId = model;
        const agent = await getAgent({ id: agentId });
        if (!agent) {
          return res.status(400).json({
            error: `智能体 "${agentId}" 不存在，请检查智能体 ID。`,
          });
        }

        // 检查 agents 端点是否已配置
        const agentsEndpointConfig = appConfig?.endpoints?.[EModelEndpoint.agents];
        if (!agentsEndpointConfig) {
          return res.status(400).json({
            error: `Agents 端点未在 Because.yaml 中配置，请检查配置。`,
          });
        }

        resolvedEndpointConfig = {
          type: 'agents',
          name: endpointName,
          endpoint: EModelEndpoint.agents,
        };
        modelConfig = { model: agent.model_parameters?.model || 'gpt-3.5-turbo' };
        agentConfig = {
          agentId,
          agent,
        };
      } else {
        // OpenAI 兼容端点
        const endpointConfig = getCustomEndpointConfig({
          endpoint: endpointName,
          appConfig,
        });
        if (!endpointConfig) {
          return res.status(400).json({
            error: `端点 "${endpointName}" 未在 Because.yaml 的 endpoints.custom 中配置，请检查配置并确保填写了 baseURL 与 apiKey。`,
          });
        }

        const rawApiKey = extractEnvVariable(endpointConfig.apiKey ?? '');
        const rawBaseURL = extractEnvVariable(endpointConfig.baseURL ?? '');
        if (rawApiKey.match(envVarRegex)) {
          return res.status(400).json({
            error: `端点 "${endpointName}" 的 API Key 未配置：请在 .env 中设置对应环境变量（如 DEEP_SEEK_API_KEY），或在 Because.yaml 中填写 apiKey。`,
          });
        }
        if (rawBaseURL.match(envVarRegex)) {
          return res.status(400).json({
            error: `端点 "${endpointName}" 的 baseURL 未配置：请在 Because.yaml 的 endpoints.custom 中为该端点填写 baseURL。`,
          });
        }

        const userProvidesKey = isUserProvided(rawApiKey);
        const userProvidesURL = isUserProvided(rawBaseURL);
        let apiKey = rawApiKey;
        let baseURL = rawBaseURL;
        if (userProvidesKey || userProvidesURL) {
          const userValues = await getUserKeyValues({ userId: req.user.id, name: endpointName });
          if (userProvidesKey) apiKey = userValues?.apiKey ?? '';
          if (userProvidesURL) baseURL = userValues?.baseURL ?? '';
        }
        if (!baseURL || !apiKey) {
          return res.status(400).json({
            error: userProvidesKey || userProvidesURL
              ? `端点 "${endpointName}" 需要用户提供 API Key 或 Base URL，请在设置中先保存后再运行基准测试。`
              : `端点 "${endpointName}" 缺少 baseURL 或 API Key，请检查 Because.yaml 与 .env 配置。`,
          });
        }

        resolvedEndpointConfig = {
          type: endpointConfig.type || 'custom',
          name: endpointName,
          baseURL,
          apiKey,
        };
        modelConfig = { model };
      }

      const finalDatasetId = datasetId || `mini_dev_${sqlDialect.toLowerCase()}`;
      const taskId = crypto.randomUUID();
      const task = {
        taskId,
        status: 'pending',
        progress: 0,
        total: 0,
        completed: 0,
        statusLogs: [],
        createdAt: new Date().toISOString(),
        cancelled: false,
        config: {
          datasetId: finalDatasetId,
          sqlDialect,
          databaseName,
          endpointConfig: resolvedEndpointConfig,
          modelConfig,
          agentConfig,
          toolsConfig: { ...toolsConfig, dataSourceId },
          evaluationMetrics,
          userId: req.user?.id,
        },
        results: null,
        error: null,
      };

      tasks.set(taskId, task);

      BenchmarkService.runBenchmarkTask(taskId, task.config, getBenchmarkRoot(), getResultsDir()).catch((error) => {
        const currentTask = tasks.get(taskId);
        if (currentTask) {
          currentTask.status = 'failed';
          currentTask.error = error.message;
          tasks.set(taskId, currentTask);
        }
      });

      res.json({
        taskId,
        status: 'pending',
        message: 'Benchmark task created successfully',
      });
    } catch (error) {
      console.error('Error creating benchmark task:', error);
      res.status(500).json({ error: error.message });
    }
  }

  static getTaskStatus(req, res) {
    try {
      const { taskId } = req.params;
      let task = tasks.get(taskId);

      if (!task) {
        task = BenchmarkController.restoreTaskFromFiles(taskId);
        if (task) tasks.set(taskId, task);
      }

      if (!task) {
        return res.status(404).json({ error: 'Task not found' });
      }

      res.json({
        taskId: task.taskId,
        status: task.status,
        progress: task.progress,
        total: task.total,
        completed: task.completed,
        statusLogs: task.statusLogs || [],
        createdAt: task.createdAt,
        error: task.error,
        currentItem: task.currentItem || null,
        itemStartTime: task.itemStartTime || null,
        lastCompletedItem: task.lastCompletedItem || null,
        lastItemDuration: task.lastItemDuration || null,
        totalDuration: getTotalDuration(task),
        results: task.status === 'completed' ? task.results : undefined,
      });
    } catch (error) {
      console.error('Error getting task status:', error);
      res.status(500).json({ error: error.message });
    }
  }

  static getResult(req, res) {
    try {
      const { taskId } = req.params;
      let task = tasks.get(taskId);

      if (!task) {
        task = BenchmarkController.restoreTaskFromFiles(taskId);
        if (task) tasks.set(taskId, task);
      }

      if (!task) {
        return res.status(404).json({ error: 'Task not found' });
      }

      if (task.status !== 'completed') {
        return res.status(400).json({
          error: 'Task is not completed yet',
          status: task.status,
        });
      }

      res.json({
        taskId: task.taskId,
        status: task.status,
        results: task.results,
        config: task.config,
        completedAt: task.completedAt,
        totalDuration: getTotalDuration(task),
      });
    } catch (error) {
      console.error('Error getting result:', error);
      res.status(500).json({ error: error.message });
    }
  }

  static getSQLComparison(req, res) {
    try {
      const fs = require('fs');
      const { taskId } = req.params;
      const resultsDir = getResultsDir();
      const predictionsPath = path.join(resultsDir, `${taskId}_predictions.json`);
      const groundTruthPath = path.join(resultsDir, `${taskId}_ground_truth.sql`);

      if (!fs.existsSync(predictionsPath) || !fs.existsSync(groundTruthPath)) {
        return res.status(404).json({ error: 'SQL comparison data not found' });
      }

      const predictions = JSON.parse(fs.readFileSync(predictionsPath, 'utf-8'));
      const groundTruthLines = fs.readFileSync(groundTruthPath, 'utf-8').split('\n').filter((line) => line.trim());
      const groundTruth = groundTruthLines.map((line, index) => {
        const parts = line.split('\t');
        return { index: index.toString(), sql: parts[0] || '', db_id: parts[1] || '' };
      });

      const comparison = Object.keys(predictions)
        .sort((a, b) => parseInt(a, 10) - parseInt(b, 10))
        .map((index) => {
          const predictedSQL = predictions[index];
          const predictedSQLClean = predictedSQL.split('\t----- bird -----\t')[0] || predictedSQL;
          const groundTruthItem = groundTruth[parseInt(index, 10)];
          return {
            index: parseInt(index, 10),
            predictedSQL: predictedSQLClean.trim(),
            groundTruthSQL: groundTruthItem ? groundTruthItem.sql.trim() : '',
            db_id: groundTruthItem ? groundTruthItem.db_id : '',
          };
        });

      res.json({ taskId, comparison });
    } catch (error) {
      console.error('Error getting SQL comparison:', error);
      res.status(500).json({ error: error.message });
    }
  }

  static restoreTaskFromFiles(taskId) {
    const fs = require('fs');
    const resultsDir = getResultsDir();
    const predictionsPath = path.join(resultsDir, `${taskId}_predictions.json`);
    const groundTruthPath = path.join(resultsDir, `${taskId}_ground_truth.sql`);

    if (!fs.existsSync(predictionsPath) || !fs.existsSync(groundTruthPath)) {
      return null;
    }

    try {
      const predictions = JSON.parse(fs.readFileSync(predictionsPath, 'utf-8'));
      const total = Object.keys(predictions).length;

      let evaluationResults = null;
      const resultJsonPath = path.join(resultsDir, `${taskId}_results.json`);
      if (fs.existsSync(resultJsonPath)) {
        try {
          evaluationResults = JSON.parse(fs.readFileSync(resultJsonPath, 'utf-8'));
        } catch (e) {
          // ignore
        }
      }

      const fileStats = fs.statSync(predictionsPath);
      const startTime = fileStats.birthtime.getTime();

      return {
        taskId,
        status: evaluationResults ? 'completed' : 'running',
        progress: evaluationResults ? 100 : 60,
        total,
        completed: total,
        statusLogs: [],
        createdAt: fileStats.birthtime.toISOString(),
        startTime,
        completedAt: evaluationResults && fs.existsSync(resultJsonPath) ? fs.statSync(resultJsonPath).mtime.toISOString() : null,
        config: { datasetId: 'unknown', sqlDialect: 'SQLite' },
        results: evaluationResults,
        error: null,
      };
    } catch (error) {
      console.error(`[BenchmarkController] Error restoring task from files: ${error.message}`);
      return null;
    }
  }

  static cancelTask(req, res) {
    try {
      const { taskId } = req.params;
      const task = tasks.get(taskId);

      if (!task) {
        return res.status(404).json({ error: 'Task not found' });
      }

      if (task.status === 'completed' || task.status === 'failed' || task.status === 'cancelled') {
        return res.status(400).json({
          error: `Task is already ${task.status}`,
        });
      }

      task.cancelled = true;
      task.status = 'cancelled';
      tasks.set(taskId, task);

      res.json({
        taskId,
        status: 'cancelled',
        message: 'Task cancelled successfully',
      });
    } catch (error) {
      console.error('Error cancelling task:', error);
      res.status(500).json({ error: error.message });
    }
  }

  static listTasks(req, res) {
    try {
      const taskList = Array.from(tasks.values()).map((task) => ({
        taskId: task.taskId,
        status: task.status,
        progress: task.progress,
        total: task.total,
        completed: task.completed,
        createdAt: task.createdAt,
        datasetId: task.config.datasetId,
        sqlDialect: task.config.sqlDialect,
      }));
      res.json({ tasks: taskList });
    } catch (error) {
      console.error('Error listing tasks:', error);
      res.status(500).json({ error: error.message });
    }
  }
}

BenchmarkController.tasks = tasks;
module.exports = BenchmarkController;
