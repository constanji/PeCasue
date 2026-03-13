const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');
const { logger } = require('@because/data-schemas');
const getConfigPath = require('~/server/utils/getConfigPath');

// 保存模型规格配置
async function saveModelSpecsConfig(req, res) {
  try {
    const { modelSpecs } = req.body;

    if (!modelSpecs) {
      return res.status(400).json({ error: 'Model specs configuration is required' });
    }

    // 读取现有的 YAML 文件
    const configPath = getConfigPath();
    logger.info(`[saveModelSpecsConfig] Using config file path: ${configPath}`);
    let config = {};

    if (fs.existsSync(configPath)) {
      try {
        const fileContents = fs.readFileSync(configPath, 'utf8');
        config = yaml.load(fileContents) || {};
      } catch (error) {
        logger.error('Error reading config file:', error);
        return res.status(500).json({ error: 'Failed to read config file' });
      }
    }

    // 更新 modelSpecs 配置
    // 如果 modelSpecs 是数组，需要包装成 { list: [...] } 格式
    // 同时保留现有配置中的其他字段（如 enforce）
    if (Array.isArray(modelSpecs)) {
      config.modelSpecs = {
        ...(config.modelSpecs || {}),
        list: modelSpecs,
      };
    } else {
      // 如果已经是对象格式，合并现有配置
      config.modelSpecs = {
        ...(config.modelSpecs || {}),
        ...modelSpecs,
      };
    }

    // 保存回 YAML 文件
    try {
      const yamlString = yaml.dump(config, {
        indent: 2,
        lineWidth: -1,
        noRefs: true,
        sortKeys: false,
        quotingType: '"',
        forceQuotes: false,
        styles: {
          '!!null': 'canonical', // 使用 ~ 表示 null
        },
      });
      
      // 检查文件是否存在，如果不存在则创建目录
      const configDir = path.dirname(configPath);
      if (!fs.existsSync(configDir)) {
        fs.mkdirSync(configDir, { recursive: true });
      }
      
      // 写入文件
      fs.writeFileSync(configPath, yamlString, 'utf8');
      
      // 验证文件是否写入成功：读取文件并验证内容
      try {
        const verifyContents = fs.readFileSync(configPath, 'utf8');
        const verifyConfig = yaml.load(verifyContents);
        
        // 验证 modelSpecs 是否真的被保存
        const savedModelSpecs = verifyConfig.modelSpecs;
        const expectedList = Array.isArray(modelSpecs) ? modelSpecs : modelSpecs.list;
        
        if (!savedModelSpecs || !savedModelSpecs.list) {
          throw new Error('Configuration was written but verification failed: modelSpecs.list not found in saved file');
        }
        
        if (savedModelSpecs.list.length !== expectedList.length) {
          logger.warn(`ModelSpecs count mismatch. Expected: ${expectedList.length}, Got: ${savedModelSpecs.list.length}`);
        }
        
        logger.info(`Model specs configuration saved and verified to ${configPath}`);
      } catch (verifyError) {
        logger.error('Error verifying config file after write:', verifyError);
        throw new Error(`File written but verification failed: ${verifyError.message}`);
      }
    } catch (error) {
      logger.error('Error writing config file:', {
        error: error.message,
        code: error.code,
        path: configPath,
        stack: error.stack,
      });
      const errorMessage = error.code === 'EACCES' 
        ? 'Permission denied: Cannot write to config file. Please check file permissions.'
        : error.code === 'ENOENT'
        ? 'Config file path does not exist.'
        : error.message || 'Failed to write config file';
      return res.status(500).json({ error: errorMessage });
    }

    // 清除缓存，强制重新加载配置（含端点与模型列表，避免编辑智能体时选择提供商后不显示可用模型）
    const { getLogStores } = require('~/cache');
    const { CacheKeys } = require('@because/data-provider');
    const cache = getLogStores(CacheKeys.CONFIG_STORE);
    await cache.delete(CacheKeys.STARTUP_CONFIG);
    await cache.delete(CacheKeys.APP_CONFIG);
    await cache.delete(CacheKeys.ENDPOINT_CONFIG);
    await cache.delete(CacheKeys.MODELS_CONFIG);

    res.setHeader('Content-Type', 'application/json');
    res.json({ success: true, message: 'Model specs configuration saved successfully' });
  } catch (error) {
    logger.error('Error saving model specs configuration:', error);
    res.status(500).json({ error: error.message || 'Failed to save model specs configuration' });
  }
}

module.exports = {
  saveModelSpecsConfig,
};

