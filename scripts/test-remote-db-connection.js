#!/usr/bin/env node
/**
 * 测试Docker容器连接远程数据库
 * 使用方法: node scripts/test-remote-db-connection.js
 */

const mysql = require('mysql2/promise');

// 配置信息（从您的截图）
const config = {
  host: '120.26.60.214',
  port: 13306,
  user: 'root',
  password: process.env.DB_PASSWORD || process.argv[2] || '', // 从环境变量或命令行参数获取密码
  database: process.argv[3] || '', // 可选的数据库名
  connectTimeout: 10000,
};

async function testConnection() {
  console.log('🔍 开始测试远程数据库连接...\n');
  console.log('配置信息:');
  console.log(`  主机: ${config.host}`);
  console.log(`  端口: ${config.port}`);
  console.log(`  用户: ${config.user}`);
  console.log(`  数据库: ${config.database || '(未指定)'}\n`);

  if (!config.password) {
    console.error('❌ 错误: 未提供密码');
    console.log('\n使用方法:');
    console.log('  1. 通过环境变量: DB_PASSWORD=your-password node scripts/test-remote-db-connection.js');
    console.log('  2. 通过命令行参数: node scripts/test-remote-db-connection.js your-password [database-name]');
    process.exit(1);
  }

  try {
    console.log('⏳ 正在连接...');
    
    // 创建连接
    const connection = await mysql.createConnection(config);
    
    console.log('✅ 连接成功！\n');
    
    // 测试ping
    await connection.ping();
    console.log('✅ Ping测试通过\n');
    
    // 获取数据库版本
    try {
      const [versionRows] = await connection.query('SELECT VERSION() as version');
      console.log(`📊 数据库版本: ${versionRows[0].version}`);
    } catch (e) {
      console.log('⚠️  无法获取版本信息:', e.message);
    }
    
    // 如果指定了数据库名，尝试切换
    if (config.database) {
      try {
        await connection.query(`USE ${config.database}`);
        console.log(`✅ 已切换到数据库: ${config.database}`);
        
        // 获取表列表
        const [tables] = await connection.query('SHOW TABLES');
        console.log(`📋 数据库表数量: ${tables.length}`);
        if (tables.length > 0 && tables.length <= 10) {
          console.log('   表列表:');
          const tableKey = Object.keys(tables[0])[0];
          tables.forEach((table, index) => {
            console.log(`   ${index + 1}. ${table[tableKey]}`);
          });
        }
      } catch (e) {
        console.log(`⚠️  无法切换到数据库 ${config.database}:`, e.message);
      }
    } else {
      // 获取所有数据库列表
      try {
        const [databases] = await connection.query('SHOW DATABASES');
        console.log(`📚 可用数据库数量: ${databases.length}`);
        if (databases.length > 0 && databases.length <= 10) {
          console.log('   数据库列表:');
          const dbKey = Object.keys(databases[0])[0];
          databases.forEach((db, index) => {
            console.log(`   ${index + 1}. ${db[dbKey]}`);
          });
        }
      } catch (e) {
        console.log('⚠️  无法获取数据库列表:', e.message);
      }
    }
    
    // 获取当前用户信息
    try {
      const [userRows] = await connection.query('SELECT USER() as current_user, DATABASE() as current_database');
      console.log(`\n👤 当前用户: ${userRows[0].current_user}`);
      console.log(`📁 当前数据库: ${userRows[0].current_database || '(无)'}`);
    } catch (e) {
      console.log('⚠️  无法获取用户信息:', e.message);
    }
    
    await connection.end();
    
    console.log('\n✅ 所有测试通过！可以在BecauseChat中配置此数据源。');
    console.log('\n📝 配置建议:');
    console.log('  1. 进入 资产管理 → 数据源管理');
    console.log('  2. 创建新数据源，填写以下信息:');
    console.log(`     主机地址: ${config.host}`);
    console.log(`     端口: ${config.port}`);
    console.log(`     用户名: ${config.user}`);
    console.log(`     密码: (您已保存的密码)`);
    if (config.database) {
      console.log(`     数据库名: ${config.database}`);
    }
    console.log('  3. 点击"测试连接"验证');
    console.log('  4. 如果远程数据库支持SSL，建议启用SSL/TLS加密');
    
  } catch (error) {
    console.error('\n❌ 连接失败！\n');
    console.error('错误信息:', error.message);
    console.error('错误代码:', error.code);
    
    // 提供常见错误的解决方案
    console.log('\n🔧 排查建议:');
    
    if (error.code === 'ETIMEDOUT' || error.code === 'ECONNREFUSED') {
      console.log('  1. 检查网络连通性:');
      console.log(`     telnet ${config.host} ${config.port}`);
      console.log('  2. 检查远程数据库是否运行');
      console.log('  3. 检查防火墙规则是否允许连接');
      console.log('  4. 检查远程数据库是否允许您的IP访问');
    } else if (error.code === 'ER_ACCESS_DENIED_ERROR' || error.code === '28000') {
      console.log('  1. 验证用户名和密码是否正确');
      console.log('  2. 检查用户是否有权限从该IP连接');
      console.log('  3. 在远程数据库上检查用户权限:');
      console.log('     SELECT user, host FROM mysql.user WHERE user=\'root\';');
    } else if (error.code === 'ENOTFOUND') {
      console.log('  1. 检查主机地址是否正确');
      console.log('  2. 检查DNS解析是否正常');
    } else {
      console.log('  1. 检查所有配置信息是否正确');
      console.log('  2. 查看远程数据库日志');
      console.log('  3. 确认数据库服务正常运行');
    }
    
    console.log('\n💡 如果是在Docker容器内测试，确保:');
    console.log('  1. 容器网络允许出站连接');
    console.log('  2. 没有防火墙阻止容器访问外部网络');
    console.log('  3. 远程数据库允许Docker宿主机的IP访问');
    
    process.exit(1);
  }
}

// 运行测试
testConnection().catch((error) => {
  console.error('未预期的错误:', error);
  process.exit(1);
});

