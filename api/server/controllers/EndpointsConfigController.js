const fs = require('fs').promises;
const path = require('path');
const yaml = require('js-yaml');
const { logger } = require('@because/data-schemas');
const { CacheKeys } = require('@because/data-provider');
const { getLogStores } = require('~/cache');
const getConfigPath = require('~/server/utils/getConfigPath');

// 获取自定义端点配置
async function getCustomEndpointsConfig(req, res) {
  try {
    const configPath = getConfigPath();

    // Check if config file exists and is a local file (not a URL)
    if (/^https?:\/\//.test(configPath)) {
      return res.status(400).json({
        error: 'Cannot read remote config file. Please use a local Because.yaml file.',
      });
    }

    // Read the current config file
    let configContent;
    try {
      configContent = await fs.readFile(configPath, 'utf8');
    } catch (error) {
      if (error.code === 'ENOENT') {
        // 如果配置文件不存在，返回空数组而不是错误
        logger.info(`[GET /config/endpoints/custom] Config file not found at ${configPath}, returning empty list`);
        return res.status(200).json({
          success: true,
          endpoints: [],
        });
      }
      throw error;
    }

    // Parse the YAML
    let config;
    try {
      config = yaml.load(configContent);
    } catch (error) {
      return res.status(400).json({ 
        error: 'Invalid YAML format in config file',
        details: error.message 
      });
    }

    // Get custom endpoints
    const customEndpoints = config.endpoints?.custom || [];

    return res.status(200).json({
      success: true,
      endpoints: customEndpoints,
    });
  } catch (err) {
    logger.error('[GET /config/endpoints/custom] Unexpected error:', err);
    return res.status(500).json({ 
      error: err.message || 'Internal server error',
    });
  }
}

// 保存自定义端点配置
async function saveCustomEndpointsConfig(req, res) {
  try {
    const { endpoint } = req.body;

    if (!endpoint || typeof endpoint !== 'object') {
      return res.status(400).json({ error: 'endpoint must be an object' });
    }

    const configPath = getConfigPath();

    // Check if config file exists and is a local file (not a URL)
    if (/^https?:\/\//.test(configPath)) {
      return res.status(400).json({
        error: 'Cannot update remote config file. Please use a local Because.yaml file.',
      });
    }

    // Read the current config file or create a new one if it doesn't exist
    let configContent;
    let config;
    try {
      configContent = await fs.readFile(configPath, 'utf8');
      // Parse the YAML
      try {
        config = yaml.load(configContent);
      } catch (error) {
        return res.status(400).json({ error: 'Invalid YAML format in config file' });
      }
    } catch (error) {
      if (error.code === 'ENOENT') {
        // 如果配置文件不存在，创建一个新的空配置
        logger.info(`[POST /config/endpoints/custom] Config file not found at ${configPath}, creating new file`);
        config = { version: '1.2.1', cache: true };
      } else {
      throw error;
    }
    }

    // Initialize endpoints.custom if it doesn't exist
    if (!config.endpoints) {
      config.endpoints = {};
    }
    if (!config.endpoints.custom) {
      config.endpoints.custom = [];
    }

    // Check if endpoint with same name already exists
    const existingIndex = config.endpoints.custom.findIndex((ep) => ep.name === endpoint.name);

    if (existingIndex >= 0) {
      // Update existing endpoint
      config.endpoints.custom[existingIndex] = endpoint;
    } else {
      // Add new endpoint
      config.endpoints.custom.push(endpoint);
    }

    // Write back to file
    const updatedYaml = yaml.dump(config, {
      indent: 2,
      lineWidth: -1,
      noRefs: true,
      sortKeys: false,
    });

    await fs.writeFile(configPath, updatedYaml, 'utf8');

    // Clear caches so endpoint list and models list refresh (fix: 编辑智能体选择提供商后不显示可用模型)
    const cache = getLogStores(CacheKeys.CONFIG_STORE);
    await cache.delete(CacheKeys.STARTUP_CONFIG);
    await cache.delete(CacheKeys.ENDPOINT_CONFIG);
    await cache.delete(CacheKeys.MODELS_CONFIG);

    logger.info(`Custom endpoint "${endpoint.name}" ${existingIndex >= 0 ? 'updated' : 'added'} successfully`);

    return res.status(200).json({
      success: true,
      message: `Custom endpoint "${endpoint.name}" ${existingIndex >= 0 ? 'updated' : 'added'} successfully`,
    });
  } catch (err) {
    logger.error('Error updating custom endpoint config', err);
    return res.status(500).json({ error: err.message });
  }
}

// 删除自定义端点配置
async function deleteCustomEndpointsConfig(req, res) {
  try {
    const { endpointName } = req.params;

    if (!endpointName) {
      return res.status(400).json({ error: 'Endpoint name is required' });
    }

    const configPath = getConfigPath();

    // Check if config file exists and is a local file (not a URL)
    if (/^https?:\/\//.test(configPath)) {
      return res.status(400).json({
        error: 'Cannot update remote config file. Please use a local Because.yaml file.',
      });
    }

    // Read the current config file
    let configContent;
    try {
      configContent = await fs.readFile(configPath, 'utf8');
    } catch (error) {
      if (error.code === 'ENOENT') {
        return res.status(404).json({ error: 'Config file not found' });
      }
      throw error;
    }

    // Parse the YAML
    let config;
    try {
      config = yaml.load(configContent);
    } catch (error) {
      return res.status(400).json({ error: 'Invalid YAML format in config file' });
    }

    // Check if endpoints.custom exists
    if (!config.endpoints?.custom || !Array.isArray(config.endpoints.custom)) {
      return res.status(404).json({ error: 'Custom endpoint not found' });
    }

    // Find and remove the endpoint
    const initialLength = config.endpoints.custom.length;
    config.endpoints.custom = config.endpoints.custom.filter((ep) => ep.name !== endpointName);

    if (config.endpoints.custom.length === initialLength) {
      return res.status(404).json({ error: `Custom endpoint "${endpointName}" not found` });
    }

    // Write back to file
    const updatedYaml = yaml.dump(config, {
      indent: 2,
      lineWidth: -1,
      noRefs: true,
      sortKeys: false,
    });

    await fs.writeFile(configPath, updatedYaml, 'utf8');

    // Clear caches so endpoint list and models list refresh
    const cache = getLogStores(CacheKeys.CONFIG_STORE);
    await cache.delete(CacheKeys.STARTUP_CONFIG);
    await cache.delete(CacheKeys.ENDPOINT_CONFIG);
    await cache.delete(CacheKeys.MODELS_CONFIG);

    logger.info(`Custom endpoint "${endpointName}" deleted successfully`);

    return res.status(200).json({
      success: true,
      message: `Custom endpoint "${endpointName}" deleted successfully`,
    });
  } catch (err) {
    logger.error('Error deleting custom endpoint config', err);
    return res.status(500).json({ error: err.message });
  }
}

module.exports = {
  getCustomEndpointsConfig,
  saveCustomEndpointsConfig,
  deleteCustomEndpointsConfig,
};

