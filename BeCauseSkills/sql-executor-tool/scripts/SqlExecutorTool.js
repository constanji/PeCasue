const { Tool } = require('@langchain/core/tools');
const { z } = require('zod');
const mysql = require('mysql2/promise');
const { Pool } = require('pg');
const { logger } = require('@because/data-schemas');
const { decryptV2 } = require('@because/api');
const path = require('path');
// 延迟加载模型函数，避免路径别名问题
let getDataSourceById = null;
let getProjectById = null;

function loadDataSourceModel() {
  if (!getDataSourceById) {
    try {
      getDataSourceById = require('~/models/DataSource').getDataSourceById;
    } catch (e) {
      getDataSourceById = require(path.resolve(__dirname, '../../../api/models/DataSource')).getDataSourceById;
    }
  }
  return getDataSourceById;
}

function loadProjectModel() {
  if (!getProjectById) {
    try {
      getProjectById = require('~/models/Project').getProjectById;
    } catch (e) {
      getProjectById = require(path.resolve(__dirname, '../../../api/models/Project')).getProjectById;
    }
  }
  return getProjectById;
}

// 连接池缓存（按数据源ID缓存）
const connectionPools = new Map();

/**
 * SQL Executor Tool - SQL执行工具（重构版）
 * 
 * 支持动态数据源切换，直接从Agent配置中获取数据源信息
 * 不再依赖独立的sql-api服务
 */
class SqlExecutorTool extends Tool {
  name = 'sql_executor';

  description =
    '执行只读的SQL SELECT查询，并返回查询结果和详细的归因分析说明。' +
    '工具会根据Agent配置的数据源自动连接对应的数据库。' +
    '支持MySQL和PostgreSQL数据库。';

  schema = z.object({
    sql: z
      .string()
      .min(1)
      .describe(
        '要执行的SQL SELECT查询语句。必须是只读查询，禁止包含INSERT/UPDATE/DELETE/DDL等写操作。',
      ),
    max_rows: z
      .number()
      .int()
      .positive()
      .max(1000)
      .optional()
      .describe('可选：限制返回的最大行数，默认返回全部结果（最多1000行）。'),
    data_source_id: z
      .string()
      .optional()
      .describe('可选：数据源ID，如果不提供则从Agent配置中获取'),
  });

  constructor(fields = {}) {
    super();
    this.req = fields.req; // 请求对象（用于获取用户信息）
    this.conversation = fields.conversation; // Conversation对象，包含project_id
  }

  /**
   * 获取数据源连接池
   */
  async getConnectionPool(dataSourceId) {
    // 清理数据源ID，确保格式正确
    const cleanedId = this.cleanDataSourceId(dataSourceId);
    
    if (!cleanedId) {
      throw new Error(`无效的数据源ID: ${dataSourceId}`);
    }

    logger.info('[SqlExecutorTool] 获取连接池，数据源ID:', { 
      original: dataSourceId, 
      cleaned: cleanedId 
    });

    // 如果已有连接池，直接返回
    if (connectionPools.has(cleanedId)) {
      return connectionPools.get(cleanedId);
    }

    // 获取数据源信息
    const getDataSourceByIdFn = loadDataSourceModel();
    const dataSource = await getDataSourceByIdFn(cleanedId);
    if (!dataSource) {
      throw new Error(`数据源不存在: ${dataSourceId}`);
    }

    // 检查数据源状态
    if (dataSource.status !== 'active') {
      throw new Error(`数据源未激活: ${dataSource.name}`);
    }

    // 解密密码
    let password;
    try {
      password = await decryptV2(dataSource.password);
    } catch (error) {
      logger.error('[SqlExecutorTool] 密码解密失败:', error);
      // 检查是否是旧格式的加密
      const parts = dataSource.password.split(':');
      if (parts.length === 3) {
        throw new Error(
          '无法解密旧格式的密码。请编辑该数据源，重新输入密码并保存配置。',
        );
      }
      throw new Error(`密码解密失败: ${error.message}`);
    }

    // 根据数据库类型创建连接池
    let pool;
    if (dataSource.type === 'mysql') {
      const poolConfig = {
        host: dataSource.host,
        port: dataSource.port,
        user: dataSource.username,
        password,
        database: dataSource.database,
        waitForConnections: true,
        connectionLimit: dataSource.connectionPool?.max || 10,
        queueLimit: 0,
        connectTimeout: dataSource.connectionPool?.connectionTimeoutMillis || 10000,
      };

      // MySQL SSL配置
      if (dataSource.ssl && dataSource.ssl.enabled) {
        poolConfig.ssl = {};
        if (dataSource.ssl.ca) {
          poolConfig.ssl.ca = dataSource.ssl.ca;
        }
        if (dataSource.ssl.cert) {
          poolConfig.ssl.cert = dataSource.ssl.cert;
        }
        if (dataSource.ssl.key) {
          poolConfig.ssl.key = dataSource.ssl.key;
        }
        if (dataSource.ssl.rejectUnauthorized !== undefined) {
          poolConfig.ssl.rejectUnauthorized = dataSource.ssl.rejectUnauthorized;
        }
      }

      pool = mysql.createPool(poolConfig);
    } else if (dataSource.type === 'postgresql') {
      const poolConfig = {
        host: dataSource.host,
        port: dataSource.port,
        user: dataSource.username,
        password,
        database: dataSource.database,
        max: dataSource.connectionPool?.max || 10,
        min: dataSource.connectionPool?.min || 0,
        idleTimeoutMillis: dataSource.connectionPool?.idleTimeoutMillis || 30000,
        connectionTimeoutMillis: dataSource.connectionPool?.connectionTimeoutMillis || 10000,
      };

      // PostgreSQL SSL配置
      if (dataSource.ssl && dataSource.ssl.enabled) {
        poolConfig.ssl = {};
        if (dataSource.ssl.rejectUnauthorized !== undefined) {
          poolConfig.ssl.rejectUnauthorized = dataSource.ssl.rejectUnauthorized;
        }
        if (dataSource.ssl.ca) {
          poolConfig.ssl.ca = dataSource.ssl.ca;
        }
        if (dataSource.ssl.cert) {
          poolConfig.ssl.cert = dataSource.ssl.cert;
        }
        if (dataSource.ssl.key) {
          poolConfig.ssl.key = dataSource.ssl.key;
        }
      }

      pool = new Pool(poolConfig);
    } else {
      throw new Error(`不支持的数据库类型: ${dataSource.type}`);
    }

    // 缓存连接池（使用清理后的ID）
    connectionPools.set(cleanedId, {
      pool,
      dataSource,
    });

    logger.info('[SqlExecutorTool] 创建连接池成功:', {
      dataSourceId: cleanedId,
      type: dataSource.type,
      database: dataSource.database,
    });

    return { pool, dataSource };
  }

  /**
   * 执行SQL查询
   */
  async executeQuery(sql, pool, dataSource) {
    if (dataSource.type === 'mysql') {
      const [rows] = await pool.execute(sql);
      return rows;
    } else if (dataSource.type === 'postgresql') {
      const result = await pool.query(sql);
      return result.rows;
    } else {
      throw new Error(`不支持的数据库类型: ${dataSource.type}`);
    }
  }

  /**
   * 从SQL语句中提取基础结构信息
   */
  extractQueryStructure(sql) {
    const upper = sql.toUpperCase();
    const structure = {
      tables: [],
      hasWhere: false,
      hasGroupBy: false,
      hasOrderBy: false,
      hasLimit: false,
    };

    try {
      // 提取FROM之后到WHERE/GROUP BY/ORDER BY/LIMIT之前的部分
      const fromMatch = upper.match(/\bFROM\b([\s\S]+?)(\bWHERE\b|\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|$)/);
      if (fromMatch && fromMatch[1]) {
        const rawTables = fromMatch[1]
          .split(',')
          .map((t) => t.trim())
          .filter(Boolean);
        structure.tables = rawTables.map((t) => t.replace(/\s+AS\s+.+$/i, '').split(/\s+/)[0]);
      }

      structure.hasWhere = /\bWHERE\b/i.test(sql);
      structure.hasGroupBy = /\bGROUP BY\b/i.test(sql);
      structure.hasOrderBy = /\bORDER BY\b/i.test(sql);
      structure.hasLimit = /\bLIMIT\b/i.test(sql);
    } catch {
      // 如果解析失败，忽略即可，用默认值
    }

    return structure;
  }

  /**
   * 构建归因分析信息
   */
  buildAttribution(sql, rows, dataSource) {
    const rowCount = Array.isArray(rows) ? rows.length : 0;
    const sampleRow = rowCount > 0 ? rows[0] : null;
    const columns = sampleRow ? Object.keys(sampleRow) : [];
    const structure = this.extractQueryStructure(sql);

    const tablePart =
      structure.tables.length > 0
        ? `主要数据来源于以下表：${structure.tables.join('，')}。`
        : '未能从SQL中可靠解析出表名，请直接结合SQL语句自行说明数据来源。';

    const clauseHints = [];
    if (structure.hasWhere) clauseHints.push('WHERE过滤条件');
    if (structure.hasGroupBy) clauseHints.push('GROUP BY分组逻辑');
    if (structure.hasOrderBy) clauseHints.push('ORDER BY排序规则');
    if (structure.hasLimit) clauseHints.push('LIMIT行数限制');

    const clausePart =
      clauseHints.length > 0
        ? `查询中包含 ${clauseHints.join('、')}，在解释结论时需要特别说明这些条件如何影响结果。`
        : '查询中未检测到WHERE/GROUP BY/ORDER BY/LIMIT等子句，结果为对全表或视图的直接查询。';

    const columnPart =
      columns.length > 0
        ? `结果中包含字段：${columns.join('，')}。在回答用户问题时，请明确指出结论分别来自哪些字段。`
        : '结果中未检测到字段列表，请在回答中先概括返回的数据结构。';

    const dataSourcePart = dataSource
      ? `数据来源于数据源"${dataSource.name}"（${dataSource.type} - ${dataSource.database}）。`
      : '';

    return {
      summary: `SQL查询已成功执行，返回${rowCount}行数据。${dataSourcePart}${tablePart}`,
      details: {
        tables: structure.tables,
        rowCount,
        columns,
        hasWhere: structure.hasWhere,
        hasGroupBy: structure.hasGroupBy,
        hasOrderBy: structure.hasOrderBy,
        hasLimit: structure.hasLimit,
      },
      guidance: [
        '1. 先用自然语言概括查询目的和结果（例如：统计某张表在特定时间范围内的记录数或明细）。',
        '2. 明确说明结论分别来自哪些表、哪些字段，以及这些字段在业务中的含义。',
        '3. 如果查询中包含WHERE/GROUP BY/ORDER BY/LIMIT等子句，逐一解释这些条件如何影响结果和结论。',
        '4. 对于数值结果，给出必要的对比或比例说明（例如：占比、同比、环比），但这些计算必须严格基于返回的数据。',
        '5. 严格禁止臆造数据库中不存在的字段或行，只能基于本次查询返回的数据进行推理和解释。',
      ],
    };
  }

  /**
   * 清理数据源ID字符串，移除多余的引号和空白字符
   */
  cleanDataSourceId(id) {
    if (!id) {
      return null;
    }
    
    // 如果是对象，尝试提取_id或id字段
    if (typeof id === 'object') {
      id = id._id || id.id || id.toString();
    }
    
    // 转换为字符串
    const str = String(id).trim();
    
    // 移除字符串两端的引号（单引号和双引号，可能有多层）
    let cleaned = str.replace(/^["']+|["']+$/g, '');
    
    // 如果还有引号，继续清理（处理双重引号的情况）
    while (cleaned !== cleaned.replace(/^["']+|["']+$/g, '')) {
      cleaned = cleaned.replace(/^["']+|["']+$/g, '');
    }
    
    return cleaned || null;
  }

  /**
   * 获取数据源ID（从输入参数、conversation.project_id或req.body）
   */
  async getDataSourceId(input) {
    // 1. 优先使用输入参数中的data_source_id
    if (input.data_source_id) {
      return this.cleanDataSourceId(input.data_source_id);
    }

    // 2. 从conversation.project_id获取项目，然后从项目获取data_source_id
    if (this.conversation && this.conversation.project_id) {
      try {
        const getProjectByIdFn = loadProjectModel();
        const projectId = this.cleanDataSourceId(this.conversation.project_id);
        const project = await getProjectByIdFn(projectId);
        if (project && project.data_source_id) {
          return project.data_source_id.toString();
        }
      } catch (error) {
        logger.warn('[SqlExecutorTool] 获取项目数据源失败:', error.message);
      }
    }

    // 3. 从conversation.data_source_id获取（如果前端直接传递了数据源ID）
    if (this.conversation && this.conversation.data_source_id) {
      return this.cleanDataSourceId(this.conversation.data_source_id);
    }

    // 4. 从req.body中获取（如果前端通过请求传递）
    if (this.req && this.req.body) {
      if (this.req.body.data_source_id) {
        return this.cleanDataSourceId(this.req.body.data_source_id);
      }
      // 如果req.body中有project_id，也尝试获取
      if (this.req.body.project_id) {
        try {
          const getProjectByIdFn = loadProjectModel();
          const projectId = this.cleanDataSourceId(this.req.body.project_id);
          const project = await getProjectByIdFn(projectId);
          if (project && project.data_source_id) {
            return project.data_source_id.toString();
          }
        } catch (error) {
          logger.warn('[SqlExecutorTool] 从req.body获取项目数据源失败:', error.message);
        }
      }
    }

    // 5. 如果都没有，返回null
    return null;
  }

  /**
   * @override
   */
  async _call(input) {
    const { sql, max_rows } = input;
    const trimmedSql = sql.trim();

    // 基础校验：只允许SELECT
    const upper = trimmedSql.toUpperCase();
    if (!upper.startsWith('SELECT')) {
      return JSON.stringify(
        {
          success: false,
          error: '只允许执行SELECT查询，请不要包含INSERT/UPDATE/DELETE/DDL等写操作。',
        },
        null,
        2,
      );
    }

    // 额外安全检查：禁止危险关键词
    const dangerousKeywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'TRUNCATE', 'CREATE'];
    for (const keyword of dangerousKeywords) {
      if (upper.includes(keyword)) {
        return JSON.stringify(
          {
            success: false,
            error: `检测到危险关键词 "${keyword}"，出于安全考虑拒绝执行该查询。`,
          },
          null,
          2,
        );
      }
    }

    try {
      // 获取数据源ID
      const dataSourceId = await this.getDataSourceId(input);

      if (!dataSourceId) {
        // 如果没有配置数据源，返回错误
        return JSON.stringify(
          {
            success: false,
            error:
              '未配置数据源。请先在左侧业务列表中选择数据源，或在调用工具时提供data_source_id参数。',
          },
          null,
          2,
        );
      }

      // 获取连接池和数据源信息
      const { pool, dataSource } = await this.getConnectionPool(dataSourceId);

      // 执行查询
      let rows = await this.executeQuery(trimmedSql, pool, dataSource);

      // 限制返回行数
      if (typeof max_rows === 'number' && max_rows > 0 && rows.length > max_rows) {
        rows = rows.slice(0, max_rows);
      }

      // 构建归因分析
      const attribution = this.buildAttribution(trimmedSql, rows, dataSource);

      const result = {
        success: true,
        sql: trimmedSql,
        rowCount: rows.length,
        rows,
        attribution,
        dataSource: {
          id: dataSource._id.toString(),
          name: dataSource.name,
          type: dataSource.type,
          database: dataSource.database,
        },
        note:
          'LLM必须基于rows和attribution进行详细的业务解释，并在回答中明确说明结论来自哪些表、哪些字段以及哪些过滤/分组/排序条件，避免任何臆造。',
      };

      logger.info('[SqlExecutorTool] SQL执行成功:', {
        dataSourceId,
        rowCount: rows.length,
        database: dataSource.database,
      });

      return JSON.stringify(result, null, 2);
    } catch (error) {
      logger.error('[SqlExecutorTool] SQL执行失败:', {
        sql: trimmedSql.substring(0, 100),
        error: error.message,
        stack: error.stack,
      });

      return JSON.stringify(
        {
          success: false,
          error: error.message || 'SQL执行失败',
          sql: trimmedSql,
        },
        null,
        2,
      );
    }
  }
}

module.exports = SqlExecutorTool;

