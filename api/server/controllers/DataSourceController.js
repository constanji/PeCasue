const crypto = require('crypto');
const mongoose = require('mongoose');
const { logger } = require('@because/data-schemas');
const { SystemRoles } = require('@because/data-provider');
const { encryptV2, decryptV2 } = require('@because/api');
const {
  createDataSource,
  getDataSources,
  getDataSourceById,
  updateDataSource,
  deleteDataSource,
} = require('~/models/DataSource');

// 使用项目统一的加密/解密函数（基于 CREDS_KEY 环境变量）
// 为了兼容旧数据，先尝试新方法，如果失败再尝试旧方法

/**
 * 加密密码（使用项目的 encryptV2）
 * @param {string} text - 要加密的文本
 * @returns {Promise<string>} 加密后的文本
 */
async function encryptPassword(text) {
  try {
    return await encryptV2(text);
  } catch (error) {
    logger.error('[encryptPassword] Error:', error);
    throw new Error('密码加密失败');
  }
}

/**
 * 解密密码（兼容新旧两种格式）
 * @param {string} encryptedText - 加密的文本
 * @returns {Promise<string>} 解密后的文本
 */
/**
 * 自定义错误类，用于标识密码解密失败的类型
 */
class PasswordDecryptionError extends Error {
  constructor(message, code = 'DECRYPTION_FAILED') {
    super(message);
    this.name = 'PasswordDecryptionError';
    this.code = code;
  }
}

async function decryptPassword(encryptedText) {
  // 首先尝试使用新方法（encryptV2格式）
  try {
    return await decryptV2(encryptedText);
  } catch (error) {
    // 如果不是新格式，尝试旧格式（AES-256-GCM格式：iv:authTag:encrypted）
    // 旧格式会有3个部分，新格式通常有2个部分（iv:encrypted）
    const parts = encryptedText.split(':');
    if (parts.length === 3) {
      // 可能是旧格式，但由于密钥已丢失，无法解密
      logger.warn('[decryptPassword] 检测到旧格式的加密数据，但无法解密。请重新输入密码。');
      throw new PasswordDecryptionError(
        '无法解密旧格式的密码。请编辑该数据源，重新输入密码并保存配置。',
        'LEGACY_ENCRYPTION_FORMAT'
      );
    }
    // 如果格式不匹配，抛出原始错误
    logger.error('[decryptPassword] Error:', error);
    throw new PasswordDecryptionError('密码解密失败：' + error.message, 'DECRYPTION_FAILED');
  }
}

/**
 * 测试数据库连接
 * @param {Object} config - 数据库配置
 * @returns {Promise<{success: boolean, error?: string}>}
 */
async function testDatabaseConnection(config) {
  const { type, host, port, database, username, password, ssl } = config;

  try {
    if (type === 'mysql') {
      // 动态加载 mysql2
      let mysql;
      try {
        mysql = require('mysql2/promise');
      } catch (error) {
        logger.error('[testDatabaseConnection] mysql2 not found:', error);
        return {
          success: false,
          error: 'mysql2 包未安装，请运行: npm install mysql2',
        };
      }

      const connectionConfig = {
        host,
        port,
        user: username,
        password,
        database,
        connectTimeout: 10000,
      };

      // MySQL SSL配置
      if (ssl && ssl.enabled) {
        connectionConfig.ssl = {};
        if (ssl.ca) {
          connectionConfig.ssl.ca = ssl.ca;
        }
        if (ssl.cert) {
          connectionConfig.ssl.cert = ssl.cert;
        }
        if (ssl.key) {
          connectionConfig.ssl.key = ssl.key;
        }
        if (ssl.rejectUnauthorized !== undefined) {
          connectionConfig.ssl.rejectUnauthorized = ssl.rejectUnauthorized;
        }
      }

      const connection = await mysql.createConnection(connectionConfig);

      await connection.ping();
      await connection.end();

      return { success: true };
    } else if (type === 'postgresql') {
      // 动态加载 pg
      let pg;
      try {
        pg = require('pg');
      } catch (error) {
        logger.error('[testDatabaseConnection] pg not found:', error);
        return {
          success: false,
          error: 'pg 包未安装，请运行: npm install pg',
        };
      }

      const { Client } = pg;
      const clientConfig = {
        host,
        port,
        database,
        user: username,
        password,
        connectionTimeoutMillis: 10000,
      };

      // PostgreSQL SSL配置
      if (ssl && ssl.enabled) {
        clientConfig.ssl = {};
        if (ssl.rejectUnauthorized !== undefined) {
          clientConfig.ssl.rejectUnauthorized = ssl.rejectUnauthorized;
        }
        if (ssl.ca) {
          clientConfig.ssl.ca = ssl.ca;
        }
        if (ssl.cert) {
          clientConfig.ssl.cert = ssl.cert;
        }
        if (ssl.key) {
          clientConfig.ssl.key = ssl.key;
        }
      }

      const client = new Client(clientConfig);

      await client.connect();
      await client.query('SELECT NOW()');
      await client.end();

      return { success: true };
    } else {
      return {
        success: false,
        error: `不支持的数据库类型: ${type}`,
      };
    }
  } catch (error) {
    logger.error('[testDatabaseConnection] Connection test failed:', error);
    return {
      success: false,
      error: error.message || '连接测试失败',
    };
  }
}

/**
 * 获取数据库结构
 * @param {Object} config - 数据库配置
 * @returns {Promise<{success: boolean, schema?: Object, error?: string}>}
 */
async function getDatabaseSchema(config) {
  const { type, host, port, database, username, password, ssl } = config;

  try {
    if (type === 'mysql') {
      let mysql;
      try {
        mysql = require('mysql2/promise');
      } catch (error) {
        return {
          success: false,
          error: 'mysql2 包未安装，请运行: npm install mysql2',
        };
      }

      const connectionConfig = {
        host,
        port,
        user: username,
        password,
        database,
        connectTimeout: 10000,
      };

      // MySQL SSL配置
      if (ssl && ssl.enabled) {
        connectionConfig.ssl = {};
        if (ssl.ca) {
          connectionConfig.ssl.ca = ssl.ca;
        }
        if (ssl.cert) {
          connectionConfig.ssl.cert = ssl.cert;
        }
        if (ssl.key) {
          connectionConfig.ssl.key = ssl.key;
        }
        if (ssl.rejectUnauthorized !== undefined) {
          connectionConfig.ssl.rejectUnauthorized = ssl.rejectUnauthorized;
        }
      }

      const connection = await mysql.createConnection(connectionConfig);

      try {
        // 获取所有表
        const [tables] = await connection.query(`
          SELECT TABLE_NAME as table_name
          FROM INFORMATION_SCHEMA.TABLES
          WHERE TABLE_SCHEMA = ?
          AND TABLE_TYPE = 'BASE TABLE'
          ORDER BY TABLE_NAME
        `, [database]);

        const schema = {};

        // 获取每个表的结构
        for (const table of tables) {
          const tableName = table.table_name;
          
          // 获取列信息
          const [columns] = await connection.query(`
            SELECT 
              COLUMN_NAME as column_name,
              DATA_TYPE as data_type,
              IS_NULLABLE as is_nullable,
              COLUMN_KEY as column_key,
              COLUMN_COMMENT as column_comment,
              COLUMN_DEFAULT as column_default
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
          `, [database, tableName]);

          // 获取索引信息
          const [indexes] = await connection.query(`
            SELECT 
              INDEX_NAME as index_name,
              COLUMN_NAME as column_name,
              NON_UNIQUE as non_unique
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY INDEX_NAME, SEQ_IN_INDEX
          `, [database, tableName]);

          schema[tableName] = {
            columns: columns.map(col => ({
              column_name: col.column_name,
              data_type: col.data_type,
              is_nullable: col.is_nullable,
              column_key: col.column_key,
              column_comment: col.column_comment || '',
              column_default: col.column_default,
            })),
            indexes: indexes.map(idx => ({
              index_name: idx.index_name,
              column_name: idx.column_name,
              non_unique: idx.non_unique,
            })),
          };
        }

        await connection.end();

        return {
          success: true,
          database,
          schema,
        };
      } catch (error) {
        await connection.end();
        throw error;
      }
    } else if (type === 'postgresql') {
      let pg;
      try {
        pg = require('pg');
      } catch (error) {
        return {
          success: false,
          error: 'pg 包未安装，请运行: npm install pg',
        };
      }

      const { Client } = pg;
      const clientConfig = {
        host,
        port,
        database,
        user: username,
        password,
        connectionTimeoutMillis: 10000,
      };

      // PostgreSQL SSL配置
      if (ssl && ssl.enabled) {
        clientConfig.ssl = {};
        if (ssl.rejectUnauthorized !== undefined) {
          clientConfig.ssl.rejectUnauthorized = ssl.rejectUnauthorized;
        }
        if (ssl.ca) {
          clientConfig.ssl.ca = ssl.ca;
        }
        if (ssl.cert) {
          clientConfig.ssl.cert = ssl.cert;
        }
        if (ssl.key) {
          clientConfig.ssl.key = ssl.key;
        }
      }

      const client = new Client(clientConfig);

      await client.connect();

      try {
        // 获取所有表
        const tablesResult = await client.query(`
          SELECT table_name
          FROM information_schema.tables
          WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
          ORDER BY table_name
        `);

        const schema = {};

        // 获取每个表的结构
        for (const row of tablesResult.rows) {
          const tableName = row.table_name;

          // 获取列信息
          const columnsResult = await client.query(`
            SELECT 
              column_name,
              data_type,
              is_nullable,
              column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = $1
            ORDER BY ordinal_position
          `, [tableName]);

          // 获取主键信息
          const pkResult = await client.query(`
            SELECT a.attname as column_name
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = $1::regclass
            AND i.indisprimary
          `, [`public.${tableName}`]);

          const primaryKeys = new Set(pkResult.rows.map(r => r.column_name));

          // 获取索引信息
          const indexesResult = await client.query(`
            SELECT
              i.relname as index_name,
              a.attname as column_name,
              ix.indisunique as is_unique
            FROM pg_class t
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE t.relkind = 'r'
            AND t.relname = $1
            ORDER BY i.relname, a.attnum
          `, [tableName]);

          schema[tableName] = {
            columns: columnsResult.rows.map(col => ({
              column_name: col.column_name,
              data_type: col.data_type,
              is_nullable: col.is_nullable === 'YES',
              column_key: primaryKeys.has(col.column_name) ? 'PRI' : '',
              column_comment: '',
              column_default: col.column_default,
            })),
            indexes: indexesResult.rows.map(idx => ({
              index_name: idx.index_name,
              column_name: idx.column_name,
              non_unique: idx.is_unique ? 0 : 1,
            })),
          };
        }

        await client.end();

        return {
          success: true,
          database,
          schema,
        };
      } catch (error) {
        await client.end();
        throw error;
      }
    } else {
      return {
        success: false,
        error: `不支持的数据库类型: ${type}`,
      };
    }
  } catch (error) {
    logger.error('[getDatabaseSchema] Error:', error);
    logger.error('[getDatabaseSchema] Error stack:', error.stack);
    logger.error('[getDatabaseSchema] Error details:', {
      message: error.message,
      name: error.name,
      code: error.code,
      errno: error.errno,
      sqlState: error.sqlState,
      sqlMessage: error.sqlMessage,
    });
    return {
      success: false,
      error: error.message || '获取数据库结构失败',
    };
  }
}

/**
 * 获取所有数据源
 * @route GET /api/config/data-sources
 * 返回所有数据源
 */
async function getDataSourcesHandler(req, res) {
  try {
    const { SystemRoles } = require('@because/data-provider');
    const isAdmin = req.user?.role === SystemRoles.ADMIN;
    
    let dataSources = await getDataSources({});

    // 如果不是管理员，只返回公开的数据源
    if (!isAdmin) {
      dataSources = dataSources.filter(ds => {
        const isPublic = ds.isPublic !== undefined ? Boolean(ds.isPublic) : false;
        return isPublic === true;
      });
    }

    // 移除密码字段，确保 isPublic 字段存在（兼容旧数据）
    const sanitizedDataSources = dataSources.map(({ password, ...rest }) => ({
      ...rest,
      isPublic: rest.isPublic !== undefined ? Boolean(rest.isPublic) : false,
    }));

    return res.status(200).json({
      success: true,
      data: sanitizedDataSources,
    });
  } catch (error) {
    logger.error('[getDataSourcesHandler] Error:', error);
    return res.status(500).json({
      success: false,
      error: error.message || '获取数据源列表失败',
    });
  }
}

/**
 * 获取单个数据源
 * @route GET /api/config/data-sources/:id
 */
async function getDataSourceHandler(req, res) {
  try {
    const { id } = req.params;
    const { id: userId } = req.user;

    const dataSource = await getDataSourceById(id);
    if (!dataSource) {
      return res.status(404).json({
        success: false,
        error: '数据源不存在',
      });
    }

    // 检查权限：只有创建者或管理员可以访问
    if (dataSource.createdBy.toString() !== userId && req.user.role !== SystemRoles.ADMIN) {
      return res.status(403).json({
        success: false,
        error: '无权访问此数据源',
      });
    }

    // 移除密码字段，确保 isPublic 字段存在（兼容旧数据）
    const { password, ...rest } = dataSource;
    const sanitizedDataSource = {
      ...rest,
      isPublic: rest.isPublic !== undefined ? Boolean(rest.isPublic) : false,
    };

    return res.status(200).json({
      success: true,
      data: sanitizedDataSource,
    });
  } catch (error) {
    logger.error('[getDataSourceHandler] Error:', error);
    return res.status(500).json({
      success: false,
      error: error.message || '获取数据源失败',
    });
  }
}

/**
 * 创建数据源
 * @route POST /api/config/data-sources
 */
async function createDataSourceHandler(req, res) {
  try {
    const { id: userId } = req.user;
    const {
      name,
      type,
      host,
      port,
      database,
      username,
      password,
      connectionPool,
      ssl,
      status = 'active',
      isPublic = false,
    } = req.body;

    // 验证必填字段
    if (!name || !type || !host || !port || !database || !username || !password) {
      return res.status(400).json({
        success: false,
        error: '缺少必填字段',
      });
    }

    // 验证类型
    if (!['mysql', 'postgresql'].includes(type)) {
      return res.status(400).json({
        success: false,
        error: '不支持的数据库类型，仅支持 mysql 和 postgresql',
      });
    }

    // 加密密码
    const encryptedPassword = await encryptPassword(password);

    // 创建数据源 - 确保userId是ObjectId类型
    // 如果connectionPool存在，确保所有字段都有值
    const poolConfig = connectionPool
      ? {
          min: connectionPool.min ?? 0,
          max: connectionPool.max ?? 10,
          idleTimeoutMillis: connectionPool.idleTimeoutMillis ?? 30000,
          connectionTimeoutMillis: connectionPool.connectionTimeoutMillis ?? 10000,
        }
      : {
          min: 0,
          max: 10,
          idleTimeoutMillis: 30000,
          connectionTimeoutMillis: 10000,
        };

    // SSL配置
    const sslConfig = ssl
      ? {
          enabled: ssl.enabled ?? false,
          rejectUnauthorized: ssl.rejectUnauthorized ?? true,
          ca: ssl.ca || null,
          cert: ssl.cert || null,
          key: ssl.key || null,
        }
      : {
          enabled: false,
          rejectUnauthorized: true,
          ca: null,
          cert: null,
          key: null,
        };

    const dataSource = await createDataSource({
      name,
      type,
      host,
      port: parseInt(port),
      database,
      username,
      password: encryptedPassword,
      connectionPool: poolConfig,
      ssl: sslConfig,
      status,
      isPublic,
      createdBy: mongoose.Types.ObjectId.isValid(userId) ? new mongoose.Types.ObjectId(userId) : userId,
    });

    // 移除密码字段 - dataSource已经是普通对象
    const { password: _, ...sanitizedDataSource } = dataSource;

    return res.status(201).json({
      success: true,
      data: sanitizedDataSource,
      message: '数据源创建成功',
    });
  } catch (error) {
    logger.error('[createDataSourceHandler] Error:', error);
    logger.error('[createDataSourceHandler] Error stack:', error.stack);
    logger.error('[createDataSourceHandler] Error details:', {
      message: error.message,
      name: error.name,
      code: error.code,
      keyPattern: error.keyPattern,
      keyValue: error.keyValue,
    });

    // 处理唯一索引冲突错误 (E11000)
    if (error.code === 11000 || error.name === 'MongoServerError') {
      const duplicateKey = error.keyPattern ? Object.keys(error.keyPattern)[0] : 'name';
      const duplicateValue = error.keyValue ? Object.values(error.keyValue)[0] : 'unknown';
      return res.status(409).json({
        success: false,
        error: `数据源名称 "${duplicateValue}" 已存在，请使用不同的名称`,
      });
    }

    // 处理验证错误
    if (error.name === 'ValidationError') {
      const validationErrors = Object.values(error.errors || {}).map((err) => err.message).join(', ');
      return res.status(400).json({
        success: false,
        error: `数据验证失败: ${validationErrors}`,
      });
    }

    // 处理类型转换错误
    if (error.name === 'CastError') {
      return res.status(400).json({
        success: false,
        error: `数据类型错误: ${error.message}`,
      });
    }

    return res.status(500).json({
      success: false,
      error: error.message || '创建数据源失败',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined,
    });
  }
}

/**
 * 更新数据源
 * @route PUT /api/config/data-sources/:id
 */
async function updateDataSourceHandler(req, res) {
  try {
    const { id } = req.params;
    const { id: userId } = req.user;
    const updateData = req.body;

    const dataSource = await getDataSourceById(id);
    if (!dataSource) {
      return res.status(404).json({
        success: false,
        error: '数据源不存在',
      });
    }

    // 检查权限
    if (dataSource.createdBy.toString() !== userId && req.user.role !== SystemRoles.ADMIN) {
      return res.status(403).json({
        success: false,
        error: '无权修改此数据源',
      });
    }

    // 如果提供了密码，需要加密
    if (updateData.password) {
      updateData.password = await encryptPassword(updateData.password);
    }

    // 如果提供了connectionPool，确保所有字段都有值
    if (updateData.connectionPool) {
      updateData.connectionPool = {
        min: updateData.connectionPool.min ?? 0,
        max: updateData.connectionPool.max ?? 10,
        idleTimeoutMillis: updateData.connectionPool.idleTimeoutMillis ?? 30000,
        connectionTimeoutMillis: updateData.connectionPool.connectionTimeoutMillis ?? 10000,
      };
    }

    // 如果提供了SSL配置，确保所有字段都有值
    if (updateData.ssl !== undefined) {
      if (updateData.ssl === null) {
        // 如果显式设置为null，则删除SSL配置
        updateData.ssl = {
          enabled: false,
          rejectUnauthorized: true,
          ca: null,
          cert: null,
          key: null,
        };
      } else {
        updateData.ssl = {
          enabled: updateData.ssl.enabled ?? false,
          rejectUnauthorized: updateData.ssl.rejectUnauthorized ?? true,
          ca: updateData.ssl.ca || null,
          cert: updateData.ssl.cert || null,
          key: updateData.ssl.key || null,
        };
      }
    }

    // 不允许修改name和createdBy
    delete updateData.name;
    delete updateData.createdBy;

    // 确保 isPublic 字段被正确设置
    if (updateData.isPublic !== undefined) {
      updateData.isPublic = Boolean(updateData.isPublic);
    }

    const updatedDataSource = await updateDataSource(id, updateData);
    if (!updatedDataSource) {
      return res.status(404).json({
        success: false,
        error: '数据源不存在',
      });
    }

    // 移除密码字段，确保 isPublic 字段存在（兼容旧数据）
    const { password: _, ...rest } = updatedDataSource;
    const sanitizedDataSource = {
      ...rest,
      isPublic: rest.isPublic !== undefined ? Boolean(rest.isPublic) : false,
    };

    return res.status(200).json({
      success: true,
      data: sanitizedDataSource,
      message: '数据源更新成功',
    });
  } catch (error) {
    logger.error('[updateDataSourceHandler] Error:', error);
    return res.status(500).json({
      success: false,
      error: error.message || '更新数据源失败',
    });
  }
}

/**
 * 删除数据源
 * @route DELETE /api/config/data-sources/:id
 */
async function deleteDataSourceHandler(req, res) {
  try {
    const { id } = req.params;
    const { id: userId } = req.user;

    const dataSource = await getDataSourceById(id);
    if (!dataSource) {
      return res.status(404).json({
        success: false,
        error: '数据源不存在',
      });
    }

    // 检查权限
    if (dataSource.createdBy.toString() !== userId && req.user.role !== SystemRoles.ADMIN) {
      return res.status(403).json({
        success: false,
        error: '无权删除此数据源',
      });
    }

    const deleted = await deleteDataSource(id);
    if (!deleted) {
      return res.status(404).json({
        success: false,
        error: '数据源不存在',
      });
    }

    return res.status(200).json({
      success: true,
      message: '数据源删除成功',
    });
  } catch (error) {
    logger.error('[deleteDataSourceHandler] Error:', error);
    return res.status(500).json({
      success: false,
      error: error.message || '删除数据源失败',
    });
  }
}

/**
 * 测试数据源连接
 * @route POST /api/config/data-sources/:id/test
 */
async function testDataSourceConnectionHandler(req, res) {
  try {
    const { id } = req.params;
    const { id: userId } = req.user;

    const dataSource = await getDataSourceById(id);
    if (!dataSource) {
      return res.status(404).json({
        success: false,
        error: '数据源不存在',
      });
    }

    // 检查权限
    if (dataSource.createdBy.toString() !== userId && req.user.role !== SystemRoles.ADMIN) {
      return res.status(403).json({
        success: false,
        error: '无权测试此数据源',
      });
    }

    // 解密密码
    let password;
    try {
      password = await decryptPassword(dataSource.password);
    } catch (decryptError) {
      // 处理密码解密错误
      if (decryptError.code === 'LEGACY_ENCRYPTION_FORMAT') {
        return res.status(400).json({
          success: false,
          status: 'error',
          error: decryptError.message,
          code: 'LEGACY_ENCRYPTION_FORMAT',
          message: decryptError.message,
        });
      }
      logger.error('[testDataSourceConnectionHandler] 密码解密失败', { error: decryptError.message });
      return res.status(500).json({
        success: false,
        status: 'error',
        error: decryptError.message || '密码解密失败',
        code: 'DECRYPTION_FAILED',
      });
    }

    // 测试连接
    const result = await testDatabaseConnection({
      type: dataSource.type,
      host: dataSource.host,
      port: dataSource.port,
      database: dataSource.database,
      username: dataSource.username,
      password,
      ssl: dataSource.ssl,
    });

    // 更新连接状态
    if (result.success) {
      await updateDataSource(id, {
        connectionStatus: 'connected',
        lastTestedAt: new Date(),
        testMessage: '连接成功',
      });
    } else {
      await updateDataSource(id, {
        connectionStatus: 'disconnected',
        lastTestedAt: new Date(),
        testMessage: result.error || '连接失败',
      });
    }

    return res.status(200).json({
      success: result.success,
      status: result.success ? 'connected' : 'disconnected',
      message: result.success ? '连接测试成功' : result.error || '连接测试失败',
    });
  } catch (error) {
    logger.error('[testDataSourceConnectionHandler] Error:', error);
    return res.status(500).json({
      success: false,
      status: 'error',
      error: error.message || '连接测试失败',
    });
  }
}

/**
 * 测试连接（用于新建数据源时）
 * @route POST /api/config/data-sources/test
 */
async function testConnectionHandler(req, res) {
  try {
    const { type, host, port, database, username, password, ssl } = req.body;

    if (!type || !host || !port || !database || !username || !password) {
      return res.status(400).json({
        success: false,
        error: '缺少必填字段',
      });
    }

    const result = await testDatabaseConnection({
      type,
      host,
      port,
      database,
      username,
      password,
      ssl,
    });

    return res.status(200).json({
      success: result.success,
      status: result.success ? 'connected' : 'disconnected',
      message: result.success ? '连接测试成功' : result.error || '连接测试失败',
    });
  } catch (error) {
    logger.error('[testConnectionHandler] Error:', error);
    return res.status(500).json({
      success: false,
      status: 'error',
      error: error.message || '连接测试失败',
    });
  }
}

/**
 * 获取数据源的数据库结构
 * @route GET /api/config/data-sources/:id/schema
 */
async function getDataSourceSchemaHandler(req, res) {
  try {
    const { id } = req.params;
    const { id: userId } = req.user;
    const isAdmin = req.user?.role === SystemRoles.ADMIN;

    logger.info('[getDataSourceSchemaHandler] 开始获取数据库结构', { id, userId });

    const dataSource = await getDataSourceById(id);
    if (!dataSource) {
      logger.warn('[getDataSourceSchemaHandler] 数据源不存在', { id });
      return res.status(404).json({
        success: false,
        error: '数据源不存在',
      });
    }

    // 检查权限：管理员可以访问所有数据源，普通用户只能访问公开的数据源
    const isPublic = dataSource.isPublic !== undefined ? Boolean(dataSource.isPublic) : false;
    const isOwner = dataSource.createdBy.toString() === userId;
    
    if (!isAdmin && !isOwner && !isPublic) {
      logger.warn('[getDataSourceSchemaHandler] 无权访问此数据源', { id, userId, createdBy: dataSource.createdBy, isPublic });
      return res.status(403).json({
        success: false,
        error: '无权访问此数据源',
      });
    }

    // 解密密码
    let password;
    try {
      password = await decryptPassword(dataSource.password);
      logger.info('[getDataSourceSchemaHandler] 密码解密成功');
    } catch (decryptError) {
      // 处理密码解密错误
      if (decryptError.code === 'LEGACY_ENCRYPTION_FORMAT') {
        return res.status(400).json({
          success: false,
          error: decryptError.message,
          code: 'LEGACY_ENCRYPTION_FORMAT',
        });
      }
      logger.error('[getDataSourceSchemaHandler] 密码解密失败', { error: decryptError.message, stack: decryptError.stack });
      return res.status(500).json({
        success: false,
        error: decryptError.message || '密码解密失败',
        code: 'DECRYPTION_FAILED',
      });
    }

    // 获取数据库结构
    logger.info('[getDataSourceSchemaHandler] 开始获取数据库结构', {
      type: dataSource.type,
      host: dataSource.host,
      port: dataSource.port,
      database: dataSource.database,
      username: dataSource.username,
    });

    const result = await getDatabaseSchema({
      type: dataSource.type,
      host: dataSource.host,
      port: dataSource.port,
      database: dataSource.database,
      username: dataSource.username,
      password,
      ssl: dataSource.ssl,
    });

    if (!result.success) {
      logger.error('[getDataSourceSchemaHandler] 获取数据库结构失败', { error: result.error });
      return res.status(500).json({
        success: false,
        error: result.error || '获取数据库结构失败',
      });
    }

    logger.info('[getDataSourceSchemaHandler] 获取数据库结构成功', {
      tableCount: Object.keys(result.schema || {}).length,
    });

    return res.status(200).json({
      success: true,
      data: result,
    });
  } catch (error) {
    logger.error('[getDataSourceSchemaHandler] Error:', error);
    logger.error('[getDataSourceSchemaHandler] Error stack:', error.stack);
    logger.error('[getDataSourceSchemaHandler] Error details:', {
      message: error.message,
      name: error.name,
      code: error.code,
    });
    return res.status(500).json({
      success: false,
      error: error.message || '获取数据库结构失败',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined,
    });
  }
}

module.exports = {
  createDataSourceHandler,
  getDataSourcesHandler,
  getDataSourceHandler,
  updateDataSourceHandler,
  deleteDataSourceHandler,
  testDataSourceConnectionHandler,
  testConnectionHandler,
  getDataSourceSchemaHandler,
  getDatabaseSchema,
  decryptPassword,
  encryptPassword,
  PasswordDecryptionError,
};
