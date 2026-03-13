---
name: database-schema-tool
description: 获取数据库的实际表结构信息（语义模型），用于SQL生成和意图判断
category: database-schema
version: 2.0
---

# Database Schema Tool

## 概述

数据库Schema工具，直接连接数据库获取完整的表结构信息，包括表名、列名、数据类型、索引等。

**重要更新（v2.0）**：
- 不再依赖独立的 `sql-api` 服务
- 直接连接数据库获取Schema，支持动态数据源切换
- 支持MySQL和PostgreSQL数据库
- 从前端业务列表选择的数据源获取Schema

**核心用途**：
- **SQL生成前**：获取数据库结构，用于生成准确的SQL查询
- **意图分类**：当RAG知识库不完整时，通过直接查询数据库结构判断查询是否与数据库相关
- **Schema查询**：回答用户关于数据库结构的一般问题

## 核心能力

1. **直接连接数据库**：不依赖sql-api服务，直接连接数据库获取真实结构
2. **动态数据源切换**：根据前端业务列表选择的数据源动态连接数据库
3. **多数据库支持**：支持MySQL和PostgreSQL
4. **语义模型格式**：支持转换为语义模型格式，直接用于text-to-sql工具
5. **灵活查询**：可以获取所有表的结构，或指定单个表的详细结构
6. **连接池管理**：使用连接池管理数据库连接，提高性能

## 输入参数

- `table` (string, 可选): 指定表名，只获取该表的结构。如果不提供，则获取所有表的结构
- `format` (enum, 可选): 输出格式，`detailed`（详细结构）或 `semantic`（语义模型格式），默认 `semantic`
- `data_source_id` (string, 可选): 数据源ID，如果不提供则从前端业务列表选择的数据源中获取

## 输出格式

### semantic 格式（默认）

```json
{
  "success": true,
  "database": "数据库名",
  "semantic_models": [
    {
      "name": "表名",
      "description": "数据库表: 表名",
      "model": "表名",
      "columns": [
        {
          "name": "列名",
          "type": "数据类型",
          "nullable": true,
          "key": "PRI|UNI|MUL",
          "comment": "列注释",
          "default": "默认值"
        }
      ],
      "indexes": []
    }
  ],
  "format": "semantic",
  "instruction": "Extract the \"semantic_models\" array from this response and use it as the semantic_models parameter when calling text-to-sql tool.",
  "dataSource": {
    "id": "数据源ID",
    "name": "数据源名称",
    "type": "mysql",
    "database": "数据库名"
  }
}
```

### detailed 格式

```json
{
  "success": true,
  "database": "数据库名",
  "schema": {
    "表名": {
      "columns": [...],
      "indexes": [...]
    }
  },
  "text_format": "可读文本格式",
  "format": "detailed",
  "dataSource": {
    "id": "数据源ID",
    "name": "数据源名称",
    "type": "mysql",
    "database": "数据库名"
  }
}
```

## 执行流程

1. **获取数据源ID**
   - 优先使用input中的`data_source_id`
   - 如果未提供，从conversation.project_id获取项目，然后从项目获取data_source_id
   - 如果conversation有data_source_id，直接使用
   - 从req.body中获取（如果前端通过请求传递）

2. **获取数据源信息**
   - 查询数据源信息（host, port, database, username, password）
   - 解密密码
   - 检查数据源状态

3. **创建数据库连接**
   - 根据数据源类型（MySQL/PostgreSQL）创建连接池
   - 使用连接池管理连接，提高性能

4. **查询数据库Schema**
   - MySQL: 查询INFORMATION_SCHEMA获取表结构
   - PostgreSQL: 查询information_schema和pg_catalog获取表结构
   - 支持获取所有表或指定单个表的结构

5. **格式转换**
   - 如果format为`semantic`，转换为语义模型格式
   - 如果format为`detailed`，返回详细结构信息

6. **返回结果**
   - 返回JSON格式的结构化数据
   - 包含成功/失败状态和错误信息

## 使用场景

### 1. SQL生成前获取Schema（主要用途）

```javascript
// 获取所有表的语义模型
const result = await database_schema({
  format: 'semantic'
});

// 提取semantic_models数组用于text-to-sql工具
const { semantic_models } = JSON.parse(result);
```

### 2. 意图分类时的后备方案

当RAG检索不到语义模型时，可以调用此工具获取数据库结构，然后检查查询是否与表/列相关：

```javascript
// 获取数据库结构
const schemaResult = await database_schema({
  format: 'semantic'
});

// 检查查询相关性
const relevance = checkQueryRelevance(userQuery, schemaData);
if (relevance.relevant) {
  // 查询与数据库相关，可能是TEXT_TO_SQL
}
```

### 3. 回答数据库结构问题

```javascript
// 获取详细结构
const result = await database_schema({
  format: 'detailed',
  table: 'users' // 可选：指定表名
});
```

## 数据源配置

数据源信息存储在MongoDB中，从前端业务列表选择的数据源获取：

1. **前端选择数据源**：用户在左侧业务列表中选择数据源
2. **数据源传递**：通过conversation.project_id或conversation.data_source_id传递
3. **自动连接**：工具自动从MongoDB获取数据源信息并连接数据库

## 与RAG检索的关系

- **RAG检索**：从知识库中检索已存储的语义模型（需要预先导入）
- **database_schema工具**：直接从数据库获取当前的真实结构（无需预先导入）

**优势**：
- 不依赖知识库的完整性
- 不依赖sql-api服务
- 始终获取最新的数据库结构
- 可以用于意图分类的后备方案

**使用建议**：
- 优先使用RAG检索（如果知识库完整）
- 如果RAG检索失败或知识库不完整，使用database_schema工具作为后备
- 在意图分类时，如果RAG检索不到语义模型，可以调用database_schema判断查询相关性

## 注意事项

- 需要提供正确的data_source_id或conversation信息
- 数据源密码是加密存储的，需要解密后才能使用
- 连接使用连接池管理，避免频繁创建连接
- 如果未配置数据源，工具会返回错误提示
- 返回的semantic_models数组可以直接用于text-to-sql工具的semantic_models参数
- 支持MySQL和PostgreSQL数据库


## 错误处理

如果获取Schema失败，返回：

```json
{
  "success": false,
  "error": "错误信息",
  "table": "表名或'all'"
}
```

常见错误：
- `未配置数据源`: 未提供data_source_id且无法从conversation获取
- `数据源不存在`: 提供的data_source_id不存在
- `数据源未激活`: 数据源状态不是active
- `密码解密失败`: 无法解密数据源密码
- `不支持的数据库类型`: 数据源类型不是mysql或postgresql

