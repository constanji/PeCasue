const { DataSource } = require('~/db/models');
const { logger } = require('@because/data-schemas');

/**
 * 创建数据源
 * @param {Object} dataSourceData - 数据源数据
 * @returns {Promise<IDataSource>}
 */
async function createDataSource(dataSourceData) {
  try {
    const dataSource = new DataSource(dataSourceData);
    const savedDataSource = await dataSource.save();
    // 返回普通对象而不是Mongoose文档
    return savedDataSource.toObject ? savedDataSource.toObject() : savedDataSource;
  } catch (error) {
    logger.error('[createDataSource] Error:', error);
    logger.error('[createDataSource] Error stack:', error.stack);
    logger.error('[createDataSource] Error details:', {
      message: error.message,
      name: error.name,
      code: error.code,
      keyPattern: error.keyPattern,
      keyValue: error.keyValue,
      errors: error.errors,
    });
    // 直接抛出原始错误，让控制器处理
    throw error;
  }
}

/**
 * 获取所有数据源
 * @param {Object} filter - 过滤条件
 * @returns {Promise<IDataSource[]>}
 */
async function getDataSources(filter = {}) {
  try {
    const dataSourceDocs = await DataSource.find(filter).sort({ createdAt: -1 });
    const dataSources = dataSourceDocs.map(doc => {
      const ds = doc.toObject ? doc.toObject() : doc;
      return {
        ...ds,
        isPublic: ds.isPublic !== undefined ? Boolean(ds.isPublic) : false,
      };
    });
    return dataSources;
  } catch (error) {
    logger.error('[getDataSources] Error:', error);
    throw error;
  }
}

/**
 * 根据ID获取数据源
 * @param {string} dataSourceId - 数据源ID
 * @returns {Promise<IDataSource | null>}
 */
async function getDataSourceById(dataSourceId) {
  try {
    const dataSourceDoc = await DataSource.findById(dataSourceId);
    if (!dataSourceDoc) {
      return null;
    }
    const dataSource = dataSourceDoc.toObject ? dataSourceDoc.toObject() : dataSourceDoc;
    return {
      ...dataSource,
      isPublic: dataSource.isPublic !== undefined ? Boolean(dataSource.isPublic) : false,
    };
  } catch (error) {
    logger.error('[getDataSourceById] Error:', error);
    throw error;
  }
}

/**
 * 更新数据源
 * @param {string} dataSourceId - 数据源ID
 * @param {Object} updateData - 更新数据
 * @returns {Promise<IDataSource | null>}
 */
async function updateDataSource(dataSourceId, updateData) {
  try {
    const update = { $set: {} };
    
    // 复制所有更新字段到 $set
    Object.keys(updateData).forEach(key => {
      if (updateData[key] !== undefined && key !== 'name' && key !== 'createdBy') {
        update.$set[key] = updateData[key];
      }
    });
    
    // 如果 isPublic 被明确设置，确保它被保存到数据库
    if (updateData.isPublic !== undefined) {
      update.$set.isPublic = Boolean(updateData.isPublic);
    }
    
    // 使用原生 MongoDB updateOne 确保字段被正确保存
    const mongoose = require('mongoose');
    const result = await DataSource.collection.updateOne(
      { _id: new mongoose.Types.ObjectId(dataSourceId) },
      update,
      { upsert: false }
    );
    
    if (result.matchedCount === 0) {
      return null;
    }
    
    // 重新查询以确保获取最新的数据
    const updatedDoc = await DataSource.findById(dataSourceId);
    if (!updatedDoc) {
      return null;
    }
    
    const updated = updatedDoc.toObject ? updatedDoc.toObject() : updatedDoc;
    
    // 确保返回的数据包含 isPublic 字段（兼容旧数据）
    if (updated.isPublic === undefined) {
      updated.isPublic = false;
    } else {
      updated.isPublic = Boolean(updated.isPublic);
    }
    
    return updated;
  } catch (error) {
    logger.error('[updateDataSource] Error:', error);
    throw error;
  }
}

/**
 * 删除数据源
 * @param {string} dataSourceId - 数据源ID
 * @returns {Promise<boolean>}
 */
async function deleteDataSource(dataSourceId) {
  try {
    const result = await DataSource.findByIdAndDelete(dataSourceId);
    return !!result;
  } catch (error) {
    logger.error('[deleteDataSource] Error:', error);
    throw error;
  }
}

module.exports = {
  createDataSource,
  getDataSources,
  getDataSourceById,
  updateDataSource,
  deleteDataSource,
};

