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
 * Database Schema Tool - 获取数据库表结构信息（重构版）
 * 
 * 直接连接数据库获取表结构信息，不再依赖独立的sql-api服务
 * 支持动态数据源切换，从前端业务列表选择的数据源获取Schema
 */
class DatabaseSchemaTool extends Tool {
  name = 'database_schema';

  description =
    '获取数据库的实际表结构信息（语义模型）。这是获取数据库Schema的主要工具，用于SQL生成和意图判断。' +
    '工具会根据前端业务列表中选择的数据源自动连接对应的数据库。' +
    '可以获取所有表的Schema，或指定单个表的详细结构。返回的信息包括表名、列名、数据类型、是否可空、主键、索引等。' +
    '使用 format="semantic" 获取语义模型格式，直接用于 text-to-sql 工具的 semantic_models 参数。' +
    '这是生成SQL查询前必须调用的工具，也可用于意图分类时判断查询是否与数据库相关。' +
    '支持MySQL和PostgreSQL数据库。';

  schema = z.object({
    table: z
      .string()
      .optional()
      .describe('可选：指定表名，只获取该表的结构。如果不提供，则获取所有表的结构'),
    format: z
      .enum(['detailed', 'semantic'])
      .optional()
      .default('semantic')
      .describe('输出格式：detailed（详细结构）或 semantic（语义模型格式，用于SQL生成），默认 semantic'),
    data_source_id: z
      .string()
      .optional()
      .describe('数据源ID，如果不提供则从前端业务列表选择的数据源中获取'),
  });

  constructor(fields = {}) {
    super();
    this.userId = fields.userId || 'system';
    this.req = fields.req;
    this.conversation = fields.conversation; // Conversation对象，包含project_id和data_source_id
  }

  /**
   * 获取数据源连接池（复用连接池以提高性能）
   */
  async getConnectionPool(dataSourceId) {
    // 清理数据源ID，确保格式正确
    const cleanedId = this.cleanDataSourceId(dataSourceId);
    
    if (!cleanedId) {
      throw new Error(`无效的数据源ID: ${dataSourceId}`);
    }

    logger.info('[DatabaseSchemaTool] 获取连接池，数据源ID:', JSON.stringify({ 
      original: String(dataSourceId || 'null'), 
      cleaned: String(cleanedId || 'null')
    }));

    // 如果已有连接池，直接返回
    if (connectionPools.has(cleanedId)) {
      const cached = connectionPools.get(cleanedId);
      return { pool: cached.pool, dataSource: cached.dataSource };
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
      logger.error('[DatabaseSchemaTool] 密码解密失败:', error);
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

    logger.info('[DatabaseSchemaTool] 创建连接池成功:', JSON.stringify({
      dataSourceId: cleanedId,
      type: dataSource.type,
      database: dataSource.database,
    }));

    return { pool, dataSource };
  }

  /**
   * 获取MySQL数据库Schema
   */
  async getMySQLSchema(pool, table = null, database = null) {
    // 如果database未提供，尝试执行查询获取当前数据库
    if (!database) {
      try {
        const [result] = await pool.execute('SELECT DATABASE() as db');
        database = result[0]?.db;
        if (!database) {
          throw new Error('无法获取数据库名：DATABASE() 返回 null');
        }
        logger.info('[DatabaseSchemaTool] 通过查询获取数据库名:', JSON.stringify({ database }));
      } catch (error) {
        logger.error('[DatabaseSchemaTool] 无法获取数据库名:', JSON.stringify({
          error: error.message,
          stack: error.stack,
        }));
        throw new Error(`无法获取数据库名: ${error.message}`);
      }
    }
    
    logger.info('[DatabaseSchemaTool] 使用MySQL数据库名:', JSON.stringify({ database }));
    
    if (table) {
      // 获取单个表的结构
      const [columns] = await pool.execute(
        `SELECT 
          COLUMN_NAME as column_name,
          DATA_TYPE as data_type,
          IS_NULLABLE as is_nullable,
          COLUMN_KEY as column_key,
          COLUMN_DEFAULT as column_default,
          COLUMN_COMMENT as column_comment
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION`,
        [database, table]
      );

      const [indexes] = await pool.execute(
        `SELECT 
          INDEX_NAME as index_name,
          COLUMN_NAME as column_name,
          NON_UNIQUE as non_unique,
          SEQ_IN_INDEX as seq_in_index
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY INDEX_NAME, SEQ_IN_INDEX`,
        [database, table]
      );

      return {
        database,
        table,
        columns: columns.map(col => ({
          ...col,
          is_nullable: col.is_nullable === 'YES',
        })),
        indexes: this.formatMySQLIndexes(indexes),
      };
    } else {
      // 获取所有表的结构
      const [tables] = await pool.execute(
        `SELECT TABLE_NAME as table_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME`,
        [database]
      );

      const schema = {};
      for (const { table_name } of tables) {
        const tableSchema = await this.getMySQLSchema(pool, table_name, database);
        schema[table_name] = {
          columns: tableSchema.columns,
          indexes: tableSchema.indexes,
        };
      }

      return {
        database,
        schema,
      };
    }
  }

  /**
   * 获取PostgreSQL数据库Schema
   */
  async getPostgreSQLSchema(pool, table = null, database = null) {
    // 如果database未提供，尝试从pool配置中获取或执行查询
    if (!database) {
      // PostgreSQL连接池的配置在pool.options中
      database = pool.options?.database;
      if (!database) {
        // 如果还是获取不到，尝试执行查询获取当前数据库
        try {
          const result = await pool.query('SELECT current_database() as db');
          database = result.rows[0]?.db;
          if (!database) {
            throw new Error('无法获取数据库名：current_database() 返回 null');
          }
          logger.info('[DatabaseSchemaTool] 通过查询获取PostgreSQL数据库名:', database);
        } catch (error) {
          logger.error('[DatabaseSchemaTool] 无法获取PostgreSQL数据库名:', JSON.stringify({
            error: error.message,
            stack: error.stack,
          }));
          throw new Error(`无法获取数据库名: ${error.message}`);
        }
      }
    }
    
    logger.info('[DatabaseSchemaTool] 使用PostgreSQL数据库名:', JSON.stringify({ database }));
    
    if (table) {
      // 获取单个表的结构
      const columnsResult = await pool.query(
        `SELECT 
          column_name,
          data_type,
          is_nullable,
          column_default,
          CASE 
            WHEN pk.column_name IS NOT NULL THEN 'PRI'
            WHEN uq.column_name IS NOT NULL THEN 'UNI'
            ELSE ''
          END as column_key,
          col_description(c.oid, a.attnum) as column_comment
        FROM information_schema.columns c
        LEFT JOIN pg_class t ON t.relname = c.table_name
        LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attname = c.column_name
        LEFT JOIN (
          SELECT ku.table_name, ku.column_name
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage ku
            ON tc.constraint_name = ku.constraint_name
          WHERE tc.constraint_type = 'PRIMARY KEY'
        ) pk ON pk.table_name = c.table_name AND pk.column_name = c.column_name
        LEFT JOIN (
          SELECT ku.table_name, ku.column_name
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage ku
            ON tc.constraint_name = ku.constraint_name
          WHERE tc.constraint_type = 'UNIQUE'
        ) uq ON uq.table_name = c.table_name AND uq.column_name = c.column_name
        WHERE c.table_schema = 'public' AND c.table_name = $1
        ORDER BY c.ordinal_position`,
        [table]
      );

      const indexesResult = await pool.query(
        `SELECT 
          i.relname as index_name,
          a.attname as column_name,
          NOT ix.indisunique as non_unique,
          array_position(ix.indkey, a.attnum) as seq_in_index
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE t.relname = $1 AND t.relkind = 'r'
        ORDER BY i.relname, seq_in_index`,
        [table]
      );

      return {
        database,
        table,
        columns: columnsResult.rows.map(col => ({
          ...col,
          is_nullable: col.is_nullable === 'YES',
        })),
        indexes: this.formatPostgreSQLIndexes(indexesResult.rows),
      };
    } else {
      // 获取所有表的结构
      const tablesResult = await pool.query(
        `SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name`
      );

      const schema = {};
      for (const { table_name } of tablesResult.rows) {
        const tableSchema = await this.getPostgreSQLSchema(pool, table_name);
        schema[table_name] = {
          columns: tableSchema.columns,
          indexes: tableSchema.indexes,
        };
      }

      return {
        database,
        schema,
      };
    }
  }

  /**
   * 格式化MySQL索引
   */
  formatMySQLIndexes(indexes) {
    const indexMap = {};
    for (const idx of indexes) {
      if (!indexMap[idx.index_name]) {
        indexMap[idx.index_name] = {
          name: idx.index_name,
          unique: idx.non_unique === 0,
          columns: [],
        };
      }
      indexMap[idx.index_name].columns.push(idx.column_name);
    }
    return Object.values(indexMap);
  }

  /**
   * 格式化PostgreSQL索引
   */
  formatPostgreSQLIndexes(indexes) {
    const indexMap = {};
    for (const idx of indexes) {
      if (!indexMap[idx.index_name]) {
        indexMap[idx.index_name] = {
          name: idx.index_name,
          unique: !idx.non_unique,
          columns: [],
        };
      }
      indexMap[idx.index_name].columns.push(idx.column_name);
    }
    return Object.values(indexMap);
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
    logger.info('[DatabaseSchemaTool] getDataSourceId 开始:', JSON.stringify({
      inputHasDataSourceId: !!input.data_source_id,
      conversationExists: !!this.conversation,
      conversationProjectId: this.conversation?.project_id || 'null',
      conversationDataSourceId: this.conversation?.data_source_id || 'null',
      reqExists: !!this.req,
      reqBodyDataSourceId: this.req?.body?.data_source_id || 'null',
      reqBodyProjectId: this.req?.body?.project_id || 'null',
    }));

    // 1. 优先使用输入参数中的data_source_id
    if (input.data_source_id) {
      const cleaned = this.cleanDataSourceId(input.data_source_id);
      logger.info('[DatabaseSchemaTool] 从input获取数据源ID:', JSON.stringify({ original: input.data_source_id, cleaned }));
      return cleaned;
    }

    // 2. 从conversation.project_id获取项目，然后从项目获取data_source_id
    if (this.conversation && this.conversation.project_id) {
      try {
        const getProjectByIdFn = loadProjectModel();
        const projectId = this.cleanDataSourceId(this.conversation.project_id);
        logger.info('[DatabaseSchemaTool] 尝试从项目获取数据源ID:', JSON.stringify({ projectId }));
        const project = await getProjectByIdFn(projectId);
        if (project && project.data_source_id) {
          const dataSourceId = project.data_source_id.toString();
          logger.info('[DatabaseSchemaTool] 从项目获取到数据源ID:', JSON.stringify({ dataSourceId }));
          return dataSourceId;
        } else {
          logger.warn('[DatabaseSchemaTool] 项目没有关联数据源:', JSON.stringify({ projectId, hasDataSourceId: !!project?.data_source_id }));
        }
      } catch (error) {
        logger.warn('[DatabaseSchemaTool] 获取项目数据源失败:', JSON.stringify({ error: error.message, stack: error.stack }));
      }
    }

    // 3. 从conversation.data_source_id获取（如果前端直接传递了数据源ID）
    if (this.conversation && this.conversation.data_source_id) {
      const cleaned = this.cleanDataSourceId(this.conversation.data_source_id);
      logger.info('[DatabaseSchemaTool] 从conversation.data_source_id获取:', JSON.stringify({ original: this.conversation.data_source_id, cleaned }));
      return cleaned;
    }

    // 4. 从req.body中获取（如果前端通过请求传递）
    if (this.req && this.req.body) {
      if (this.req.body.data_source_id) {
        const cleaned = this.cleanDataSourceId(this.req.body.data_source_id);
        logger.info('[DatabaseSchemaTool] 从req.body.data_source_id获取:', JSON.stringify({ original: this.req.body.data_source_id, cleaned }));
        return cleaned;
      }
      // 如果req.body中有project_id，也尝试获取
      if (this.req.body.project_id) {
        try {
          const getProjectByIdFn = loadProjectModel();
          const projectId = this.cleanDataSourceId(this.req.body.project_id);
          logger.info('[DatabaseSchemaTool] 尝试从req.body项目获取数据源ID:', JSON.stringify({ projectId }));
          const project = await getProjectByIdFn(projectId);
          if (project && project.data_source_id) {
            const dataSourceId = project.data_source_id.toString();
            logger.info('[DatabaseSchemaTool] 从req.body项目获取到数据源ID:', JSON.stringify({ dataSourceId }));
            return dataSourceId;
          }
        } catch (error) {
          logger.warn('[DatabaseSchemaTool] 从req.body获取项目数据源失败:', JSON.stringify({ error: error.message, stack: error.stack }));
        }
      }
    }

    // 5. 如果都没有，返回null
    logger.warn('[DatabaseSchemaTool] 未找到数据源ID，返回null');
    return null;
  }

  /**
   * 转换为语义模型格式
   */
  convertToSemanticModel(schemaData) {
    if (!schemaData.schema && !schemaData.columns) {
      return [];
    }

    // 单个表的情况
    if (schemaData.columns) {
      return [
        {
          name: schemaData.table,
          description: `数据库表: ${schemaData.table}`,
          model: schemaData.table,
          columns: schemaData.columns.map((col) => ({
            name: col.column_name,
            type: col.data_type,
            nullable: col.is_nullable === 'YES' || col.is_nullable === true,
            key: col.column_key,
            comment: col.column_comment || '',
            default: col.column_default,
          })),
          indexes: schemaData.indexes || [],
        },
      ];
    }

    // 多个表的情况
    const semanticModels = [];
    for (const [tableName, tableInfo] of Object.entries(schemaData.schema)) {
      semanticModels.push({
        name: tableName,
        description: `数据库表: ${tableName}`,
        model: tableName,
        columns: tableInfo.columns.map((col) => ({
          name: col.column_name,
          type: col.data_type,
          nullable: col.is_nullable === 'YES' || col.is_nullable === true,
          key: col.column_key,
          comment: col.column_comment || '',
          default: col.column_default,
        })),
        indexes: tableInfo.indexes || [],
      });
    }

    return semanticModels;
  }

  /**
   * 格式化输出为可读文本
   */
  formatAsText(schemaData) {
    if (!schemaData.schema && !schemaData.columns) {
      return '未找到表结构信息';
    }

    let output = `数据库: ${schemaData.database}\n\n`;

    // 单个表的情况
    if (schemaData.columns) {
      output += `表名: ${schemaData.table}\n`;
      output += '列信息:\n';
      schemaData.columns.forEach((col) => {
        output += `  - ${col.column_name} (${col.data_type})`;
        if (col.column_key === 'PRI') output += ' [主键]';
        if (col.column_key === 'UNI') output += ' [唯一]';
        if (col.is_nullable === 'NO' || col.is_nullable === false) output += ' [非空]';
        if (col.column_comment) output += ` - ${col.column_comment}`;
        output += '\n';
      });
      return output;
    }

    // 多个表的情况
    for (const [tableName, tableInfo] of Object.entries(schemaData.schema)) {
      output += `表名: ${tableName}\n`;
      output += '列信息:\n';
      tableInfo.columns.forEach((col) => {
        output += `  - ${col.column_name} (${col.data_type})`;
        if (col.column_key === 'PRI') output += ' [主键]';
        if (col.column_key === 'UNI') output += ' [唯一]';
        if (col.is_nullable === 'NO' || col.is_nullable === false) output += ' [非空]';
        if (col.column_comment) output += ` - ${col.column_comment}`;
        output += '\n';
      });
      output += '\n';
    }

    return output;
  }

  /**
   * 检查查询是否与数据库表/列相关（用于意图分类）
   */
  checkQueryRelevance(query, schemaData) {
    const queryLower = query.toLowerCase();
    const tableNames = [];
    
    // 提取所有表名
    if (schemaData.schema) {
      tableNames.push(...Object.keys(schemaData.schema));
    } else if (schemaData.table) {
      tableNames.push(schemaData.table);
    }

    // 提取所有列名
    const columnNames = [];
    if (schemaData.schema) {
      for (const tableInfo of Object.values(schemaData.schema)) {
        if (tableInfo.columns) {
          columnNames.push(...tableInfo.columns.map(col => col.column_name.toLowerCase()));
        }
      }
    } else if (schemaData.columns) {
      columnNames.push(...schemaData.columns.map(col => col.column_name.toLowerCase()));
    }

    // 检查查询中是否包含表名或列名
    const matchedTables = tableNames.filter(tableName => 
      queryLower.includes(tableName.toLowerCase())
    );
    const matchedColumns = columnNames.filter(columnName => 
      queryLower.includes(columnName)
    );

    return {
      relevant: matchedTables.length > 0 || matchedColumns.length > 0,
      matched_tables: matchedTables,
      matched_columns: matchedColumns.slice(0, 5), // 只返回前5个匹配的列
    };
  }

  /**
   * @override
   */
  async _call(input) {
    const { table, format = 'semantic' } = input;
    let dataSourceId = null; // 在外部作用域声明，确保catch块可以访问

    try {
      logger.info('[DatabaseSchemaTool] _call 开始:', JSON.stringify({ 
        input: JSON.stringify(input),
        table: table || 'all', 
        format,
        hasConversation: !!this.conversation,
        hasReq: !!this.req,
      }));

      // 获取数据源ID
      logger.info('[DatabaseSchemaTool] 准备调用 getDataSourceId');
      dataSourceId = await this.getDataSourceId(input);
      logger.info('[DatabaseSchemaTool] getDataSourceId 返回:', JSON.stringify({ dataSourceId: dataSourceId || 'null' }));

      logger.info('[DatabaseSchemaTool] 获取到的数据源ID:', JSON.stringify({ 
        dataSourceId: dataSourceId || 'null',
        conversation: this.conversation ? {
          project_id: this.conversation.project_id || 'null',
          data_source_id: this.conversation.data_source_id || 'null',
        } : 'conversation is null',
        reqBody: this.req?.body ? {
          project_id: this.req.body.project_id || 'null',
          data_source_id: this.req.body.data_source_id || 'null',
        } : 'req.body is null',
      }));

      if (!dataSourceId) {
        const errorMsg = '未配置数据源。请先在左侧业务列表中选择数据源，或在调用工具时提供data_source_id参数。';
        logger.warn('[DatabaseSchemaTool]', errorMsg);
        return JSON.stringify({
          success: false,
          error: errorMsg,
          table: table || 'all',
        });
      }

      // 获取连接池和数据源信息
      const { pool, dataSource } = await this.getConnectionPool(dataSourceId);

      // 根据数据库类型获取Schema
      let schemaData;
      try {
        logger.info('[DatabaseSchemaTool] 开始获取Schema:', JSON.stringify({ 
          databaseType: dataSource.type, 
          database: dataSource.database,
          table: table || 'all',
        }));
        if (dataSource.type === 'mysql') {
          schemaData = await this.getMySQLSchema(pool, table, dataSource.database);
        } else if (dataSource.type === 'postgresql') {
          schemaData = await this.getPostgreSQLSchema(pool, table, dataSource.database);
        } else {
          throw new Error(`不支持的数据库类型: ${dataSource.type}`);
        }
      } catch (schemaError) {
        logger.error('[DatabaseSchemaTool] 获取Schema时出错:', JSON.stringify({
          error: schemaError.message,
          stack: schemaError.stack,
          databaseType: dataSource.type,
          database: dataSource.database,
          table: table || 'all',
          dataSourceId: dataSourceId || 'null',
        }));
        throw schemaError;
      }

      // 根据格式返回
      if (format === 'semantic') {
        const semanticModels = this.convertToSemanticModel(schemaData);
        // 返回清晰的格式，方便主代理提取 semantic_models
        return JSON.stringify(
          {
            success: true,
            database: schemaData.database,
            semantic_models: semanticModels,
            format: 'semantic',
            instruction: 'Extract the "semantic_models" array from this response and use it as the semantic_models parameter when calling text-to-sql tool.',
            dataSource: {
              id: dataSource._id.toString(),
              name: dataSource.name,
              type: dataSource.type,
              database: dataSource.database,
            },
          },
          null,
          2,
        );
      } else {
        // detailed 格式
        return JSON.stringify(
          {
            success: true,
            database: schemaData.database,
            schema: schemaData.schema || { [schemaData.table]: { columns: schemaData.columns, indexes: schemaData.indexes } },
            text_format: this.formatAsText(schemaData),
            format: 'detailed',
            dataSource: {
              id: dataSource._id.toString(),
              name: dataSource.name,
              type: dataSource.type,
              database: dataSource.database,
            },
          },
          null,
          2,
        );
      }
    } catch (error) {
      const errorInfo = {
        table: table || 'all',
        error: error.message || '未知错误',
        stack: error.stack || '无堆栈信息',
        dataSourceId: dataSourceId || 'null',
        conversationExists: !!this.conversation,
        reqExists: !!this.req,
        input: JSON.stringify(input),
      };
      
      logger.error('[DatabaseSchemaTool] 获取Schema失败:', JSON.stringify(errorInfo, null, 2));

      return JSON.stringify({
        success: false,
        error: error.message || '获取Schema失败',
        table: table || 'all',
        details: process.env.NODE_ENV === 'development' ? errorInfo : undefined,
      });
    }
  }
}

module.exports = DatabaseSchemaTool;
