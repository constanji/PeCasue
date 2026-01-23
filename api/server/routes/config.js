const express = require('express');
const { logger } = require('@because/data-schemas');
const { isEnabled, getBalanceConfig } = require('@because/api');
const {
  Constants,
  CacheKeys,
  removeNullishValues,
  defaultSocialLogins,
} = require('@because/data-provider');
const { getLdapConfig } = require('~/server/services/Config/ldap');
const { getAppConfig } = require('~/server/services/Config/app');
const { getProjectByName } = require('~/models/Project');
const { getMCPManager } = require('~/config');
const { getLogStores } = require('~/cache');
const { mcpServersRegistry } = require('@because/api');
const { requireJwtAuth, checkAdmin } = require('~/server/middleware');

const router = express.Router();
const updateInterfaceConfig = require('~/server/controllers/InterfaceController');
const {
  getCustomEndpointsConfig,
  saveCustomEndpointsConfig,
  deleteCustomEndpointsConfig,
} = require('~/server/controllers/EndpointsConfigController');
const {
  getCustomMCPServersConfig,
  saveCustomMCPServersConfig,
  deleteCustomMCPServersConfig,
} = require('~/server/controllers/MCPConfigController');
const {
  getAgentPromptsConfig,
  saveAgentPromptsConfig,
} = require('~/server/controllers/AgentPromptsController');
const {
  getDataSourcesHandler,
  getDataSourceHandler,
  createDataSourceHandler,
  updateDataSourceHandler,
  deleteDataSourceHandler,
  testDataSourceConnectionHandler,
  testConnectionHandler,
  getDataSourceSchemaHandler,
} = require('~/server/controllers/DataSourceController');
const {
  generateSemanticModelHandler,
} = require('~/server/controllers/SemanticModelController');
const {
  saveModelSpecsConfig,
} = require('~/server/controllers/ModelSpecsController');
const {
  listProjectsHandler,
  getProjectHandler,
  updateProjectDataSourceHandler,
} = require('~/server/controllers/ProjectController');
const emailLoginEnabled =
  process.env.ALLOW_EMAIL_LOGIN === undefined || isEnabled(process.env.ALLOW_EMAIL_LOGIN);
const passwordResetEnabled = isEnabled(process.env.ALLOW_PASSWORD_RESET);

const sharedLinksEnabled =
  process.env.ALLOW_SHARED_LINKS === undefined || isEnabled(process.env.ALLOW_SHARED_LINKS);

const publicSharedLinksEnabled =
  sharedLinksEnabled &&
  (process.env.ALLOW_SHARED_LINKS_PUBLIC === undefined ||
    isEnabled(process.env.ALLOW_SHARED_LINKS_PUBLIC));

const sharePointFilePickerEnabled = isEnabled(process.env.ENABLE_SHAREPOINT_FILEPICKER);
const openidReuseTokens = isEnabled(process.env.OPENID_REUSE_TOKENS);

/**
 * Fetches MCP servers from registry and adds them to the payload.
 * Registry now includes all configured servers (from YAML) plus inspection data when available.
 * Always fetches fresh to avoid caching incomplete initialization state.
 */
const getMCPServers = async (payload, appConfig) => {
  try {
    if (appConfig?.mcpConfig == null) {
      return;
    }
    const mcpManager = getMCPManager();
    if (!mcpManager) {
      return;
    }
    const mcpServers = await mcpServersRegistry.getAllServerConfigs();
    if (!mcpServers) return;
    for (const serverName in mcpServers) {
      if (!payload.mcpServers) {
        payload.mcpServers = {};
      }
      const serverConfig = mcpServers[serverName];
      payload.mcpServers[serverName] = removeNullishValues({
        startup: serverConfig?.startup,
        chatMenu: serverConfig?.chatMenu,
        isOAuth: serverConfig.requiresOAuth,
        customUserVars: serverConfig?.customUserVars,
      });
    }
  } catch (error) {
    logger.error('Error loading MCP servers', error);
  }
};

router.get('/', async function (req, res) {
  const cache = getLogStores(CacheKeys.CONFIG_STORE);

  const cachedStartupConfig = await cache.get(CacheKeys.STARTUP_CONFIG);
  if (cachedStartupConfig) {
    const appConfig = await getAppConfig({ role: req.user?.role });
    await getMCPServers(cachedStartupConfig, appConfig);
    res.send(cachedStartupConfig);
    return;
  }

  const isBirthday = () => {
    const today = new Date();
    return today.getMonth() === 1 && today.getDate() === 11;
  };

  const instanceProject = await getProjectByName(Constants.GLOBAL_PROJECT_NAME, '_id');

  const ldap = getLdapConfig();

  try {
    const appConfig = await getAppConfig({ role: req.user?.role });

    const isOpenIdEnabled =
      !!process.env.OPENID_CLIENT_ID &&
      !!process.env.OPENID_CLIENT_SECRET &&
      !!process.env.OPENID_ISSUER &&
      !!process.env.OPENID_SESSION_SECRET;

    const isSamlEnabled =
      !!process.env.SAML_ENTRY_POINT &&
      !!process.env.SAML_ISSUER &&
      !!process.env.SAML_CERT &&
      !!process.env.SAML_SESSION_SECRET;

    const balanceConfig = getBalanceConfig(appConfig);

    /** @type {TStartupConfig} */
    const payload = {
      appTitle: process.env.APP_TITLE || 'Because',
      socialLogins: appConfig?.registration?.socialLogins ?? defaultSocialLogins,
      discordLoginEnabled: !!process.env.DISCORD_CLIENT_ID && !!process.env.DISCORD_CLIENT_SECRET,
      facebookLoginEnabled:
        !!process.env.FACEBOOK_CLIENT_ID && !!process.env.FACEBOOK_CLIENT_SECRET,
      githubLoginEnabled: !!process.env.GITHUB_CLIENT_ID && !!process.env.GITHUB_CLIENT_SECRET,
      googleLoginEnabled: !!process.env.GOOGLE_CLIENT_ID && !!process.env.GOOGLE_CLIENT_SECRET,
      appleLoginEnabled:
        !!process.env.APPLE_CLIENT_ID &&
        !!process.env.APPLE_TEAM_ID &&
        !!process.env.APPLE_KEY_ID &&
        !!process.env.APPLE_PRIVATE_KEY_PATH,
      openidLoginEnabled: isOpenIdEnabled,
      openidLabel: process.env.OPENID_BUTTON_LABEL || 'Continue with OpenID',
      openidImageUrl: process.env.OPENID_IMAGE_URL,
      openidAutoRedirect: isEnabled(process.env.OPENID_AUTO_REDIRECT),
      samlLoginEnabled: !isOpenIdEnabled && isSamlEnabled,
      samlLabel: process.env.SAML_BUTTON_LABEL,
      samlImageUrl: process.env.SAML_IMAGE_URL,
      serverDomain: process.env.DOMAIN_SERVER || 'http://localhost:3080',
      emailLoginEnabled,
      registrationEnabled: !ldap?.enabled && isEnabled(process.env.ALLOW_REGISTRATION),
      socialLoginEnabled: isEnabled(process.env.ALLOW_SOCIAL_LOGIN),
      emailEnabled:
        (!!process.env.EMAIL_SERVICE || !!process.env.EMAIL_HOST) &&
        !!process.env.EMAIL_USERNAME &&
        !!process.env.EMAIL_PASSWORD &&
        !!process.env.EMAIL_FROM,
      passwordResetEnabled,
      showBirthdayIcon:
        isBirthday() ||
        isEnabled(process.env.SHOW_BIRTHDAY_ICON) ||
        process.env.SHOW_BIRTHDAY_ICON === '',
      helpAndFaqURL: process.env.HELP_AND_FAQ_URL || 'https://because.ai',
      interface: appConfig?.interfaceConfig,
      turnstile: appConfig?.turnstileConfig,
      modelSpecs: appConfig?.modelSpecs,
      balance: balanceConfig,
      sharedLinksEnabled,
      publicSharedLinksEnabled,
      analyticsGtmId: process.env.ANALYTICS_GTM_ID,
      instanceProjectId: instanceProject._id.toString(),
      bundlerURL: process.env.SANDPACK_BUNDLER_URL,
      staticBundlerURL: process.env.SANDPACK_STATIC_BUNDLER_URL,
      sharePointFilePickerEnabled,
      sharePointBaseUrl: process.env.SHAREPOINT_BASE_URL,
      sharePointPickerGraphScope: process.env.SHAREPOINT_PICKER_GRAPH_SCOPE,
      sharePointPickerSharePointScope: process.env.SHAREPOINT_PICKER_SHAREPOINT_SCOPE,
      openidReuseTokens,
      conversationImportMaxFileSize: process.env.CONVERSATION_IMPORT_MAX_FILE_SIZE_BYTES
        ? parseInt(process.env.CONVERSATION_IMPORT_MAX_FILE_SIZE_BYTES, 10)
        : 0,
    };

    const minPasswordLength = parseInt(process.env.MIN_PASSWORD_LENGTH, 10);
    if (minPasswordLength && !isNaN(minPasswordLength)) {
      payload.minPasswordLength = minPasswordLength;
    }

    const webSearchConfig = appConfig?.webSearch;
    if (
      webSearchConfig != null &&
      (webSearchConfig.searchProvider ||
        webSearchConfig.scraperProvider ||
        webSearchConfig.rerankerType)
    ) {
      payload.webSearch = {};
    }

    if (webSearchConfig?.searchProvider) {
      payload.webSearch.searchProvider = webSearchConfig.searchProvider;
    }
    if (webSearchConfig?.scraperProvider) {
      payload.webSearch.scraperProvider = webSearchConfig.scraperProvider;
    }
    if (webSearchConfig?.rerankerType) {
      payload.webSearch.rerankerType = webSearchConfig.rerankerType;
    }

    if (ldap) {
      payload.ldap = ldap;
    }

    if (typeof process.env.CUSTOM_FOOTER === 'string') {
      payload.customFooter = process.env.CUSTOM_FOOTER;
    }

    // 从数据库加载 agentPrompts 配置
    try {
      const models = require('~/db/models');
      const { AgentPromptsConfig } = models;
      if (AgentPromptsConfig) {
        const agentPromptsConfig = await AgentPromptsConfig.findOne({ configId: 'default' }).lean();
        if (agentPromptsConfig) {
          const { _id, configId, ...agentPrompts } = agentPromptsConfig;
          logger.info('[startup config] Loaded agentPrompts from database:', JSON.stringify(agentPrompts, null, 2));
          payload.agentPrompts = agentPrompts;
        } else {
          logger.info('[startup config] No agentPrompts config found in database');
        }
      } else {
        logger.warn('AgentPromptsConfig model not found, available models:', Object.keys(models));
      }
    } catch (error) {
      logger.warn('Error loading agentPrompts config from database', error);
      // 如果数据库加载失败，尝试从 YAML 文件加载（向后兼容）
      if (appConfig?.config?.agentPrompts) {
        payload.agentPrompts = appConfig.config.agentPrompts;
      }
    }

    await cache.set(CacheKeys.STARTUP_CONFIG, payload);
    await getMCPServers(payload, appConfig);
    return res.status(200).send(payload);
  } catch (err) {
    logger.error('Error in startup config', err);
    return res.status(500).send({ error: err.message });
  }
});

// 保存 interface 配置（需要管理员权限）
router.post('/interface', requireJwtAuth, checkAdmin, updateInterfaceConfig);

// 模型规格配置路由（需要管理员权限）
router.post('/modelSpecs', requireJwtAuth, checkAdmin, saveModelSpecsConfig);

// 端点配置路由（需要管理员权限）
router.get('/endpoints/custom', requireJwtAuth, checkAdmin, getCustomEndpointsConfig);
router.post('/endpoints/custom', requireJwtAuth, checkAdmin, saveCustomEndpointsConfig);
router.delete('/endpoints/custom/:endpointName', requireJwtAuth, checkAdmin, deleteCustomEndpointsConfig);

// MCP 服务器配置路由（需要管理员权限）
router.get('/mcp/custom', requireJwtAuth, checkAdmin, getCustomMCPServersConfig);
router.post('/mcp/custom', requireJwtAuth, checkAdmin, saveCustomMCPServersConfig);
router.delete('/mcp/custom/:serverName', requireJwtAuth, checkAdmin, deleteCustomMCPServersConfig);

// 智能体提示集配置路由（需要管理员权限）
router.get('/agent-prompts', requireJwtAuth, checkAdmin, getAgentPromptsConfig);
router.post('/agent-prompts', requireJwtAuth, checkAdmin, saveAgentPromptsConfig);

// 数据源配置路由
// GET 接口：所有已认证用户都可以访问，但普通用户只能看到公开的数据源
router.get('/data-sources', requireJwtAuth, getDataSourcesHandler);
router.get('/data-sources/:id', requireJwtAuth, checkAdmin, getDataSourceHandler);
router.post('/data-sources', requireJwtAuth, checkAdmin, createDataSourceHandler);
router.put('/data-sources/:id', requireJwtAuth, checkAdmin, updateDataSourceHandler);
router.delete('/data-sources/:id', requireJwtAuth, checkAdmin, deleteDataSourceHandler);
router.post('/data-sources/:id/test', requireJwtAuth, checkAdmin, testDataSourceConnectionHandler);
router.post('/data-sources/test', requireJwtAuth, checkAdmin, testConnectionHandler);
// GET schema 接口：所有已认证用户都可以访问，但普通用户只能查看公开数据源的结构
router.get('/data-sources/:id/schema', requireJwtAuth, getDataSourceSchemaHandler);
router.post('/data-sources/:id/generate-semantic-model', requireJwtAuth, checkAdmin, generateSemanticModelHandler);

// 项目查询路由（需要认证，但不需要管理员权限）
router.get('/projects', requireJwtAuth, listProjectsHandler);
router.get('/projects/:id', requireJwtAuth, getProjectHandler);
router.put('/projects/:id/data-source', requireJwtAuth, updateProjectDataSourceHandler);

module.exports = router;
