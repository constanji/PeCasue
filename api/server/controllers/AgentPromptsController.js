const { logger } = require('@because/data-schemas');
const { CacheKeys } = require('@because/data-provider');
const { getLogStores } = require('~/cache');

/**
 * 获取 AgentPromptsConfig 模型（动态加载以确保模型已初始化）
 */
function getAgentPromptsConfigModel() {
  try {
    const models = require('~/db/models');
    const { AgentPromptsConfig } = models;
    if (!AgentPromptsConfig) {
      logger.error('[AgentPromptsController] AgentPromptsConfig model not found in models:', Object.keys(models));
      return null;
    }
    return AgentPromptsConfig;
  } catch (error) {
    logger.error('[AgentPromptsController] Error loading models:', error);
    return null;
  }
}

/**
 * 获取智能体提示集配置（从数据库）
 * @route GET /api/config/agent-prompts
 */
async function getAgentPromptsConfig(req, res) {
  try {
    const AgentPromptsConfig = getAgentPromptsConfigModel();
    if (!AgentPromptsConfig) {
      logger.error('[getAgentPromptsConfig] AgentPromptsConfig model is undefined');
      return res.status(500).json({
        error: 'AgentPromptsConfig model not initialized. Please restart the server.',
      });
    }

    // 从数据库获取配置，如果不存在则返回空配置
    const config = await AgentPromptsConfig.findOne({ configId: 'default' }).lean();

    if (!config) {
      return res.json({ agentPrompts: null });
    }

    // 返回配置数据（排除 _id 和 configId）
    const { _id, configId, ...agentPrompts } = config;
    return res.json({ agentPrompts });
  } catch (error) {
    logger.error('[/api/config/agent-prompts] Error getting agent prompts config', error);
    return res.status(500).json({
      error: error.message || 'Internal server error',
    });
  }
}

/**
 * 保存智能体提示集配置（到数据库）
 * @route POST /api/config/agent-prompts
 */
async function saveAgentPromptsConfig(req, res) {
  try {
    const AgentPromptsConfig = getAgentPromptsConfigModel();
    if (!AgentPromptsConfig) {
      logger.error('[saveAgentPromptsConfig] AgentPromptsConfig model is undefined');
      return res.status(500).json({
        error: 'AgentPromptsConfig model not initialized. Please restart the server.',
      });
    }

    const { agentPrompts } = req.body;

    if (!agentPrompts || typeof agentPrompts !== 'object') {
      return res.status(400).json({ error: 'agentPrompts must be an object' });
    }

    // 调试：打印接收到的数据
    logger.info('[saveAgentPromptsConfig] Received agentPrompts:', JSON.stringify(agentPrompts, null, 2));

    // 使用 upsert 操作：如果存在则更新，不存在则创建
    const updateData = {
      configId: 'default',
      ...agentPrompts,
    };

    logger.info('[saveAgentPromptsConfig] Update data:', JSON.stringify(updateData, null, 2));

    const config = await AgentPromptsConfig.findOneAndUpdate(
      { configId: 'default' },
      updateData,
      {
        upsert: true,
        new: true,
        setDefaultsOnInsert: true,
      },
    );

    logger.info('[saveAgentPromptsConfig] Saved config:', JSON.stringify(config.toObject(), null, 2));

    // 清除启动配置缓存
    const cache = getLogStores(CacheKeys.CONFIG_STORE);
    await cache.delete(CacheKeys.STARTUP_CONFIG);

    logger.info('[POST /api/config/agent-prompts] Agent prompts config saved successfully');
    return res.json({ success: true, message: 'Agent prompts config saved successfully' });
  } catch (error) {
    logger.error('[/api/config/agent-prompts] Error saving agent prompts config', error);
    return res.status(500).json({
      error: error.message || 'Internal server error',
    });
  }
}

module.exports = {
  getAgentPromptsConfig,
  saveAgentPromptsConfig,
};

