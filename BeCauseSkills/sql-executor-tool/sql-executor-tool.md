---
name: sql-executor-tool
description: 执行SQL查询并返回结果和归因分析，支持动态数据源切换
category: sql-execution
version: 2.1
---

# SQL Executor Tool

## 概述

SQL执行工具，直接连接数据库执行SQL查询，支持动态数据源切换。

**重要更新（v2.0）**：
- 不再依赖独立的 `sql-api` 服务
- 直接从Agent配置中获取数据源信息
- 支持MySQL和PostgreSQL数据库
- 动态创建数据库连接，支持多数据源切换
- ✅ **支持WITH子句（CTE）**：完整支持WITH子句和复杂SQL查询

## 核心能力

1. **动态数据源切换**：根据Agent配置的 `data_source_id` 动态连接数据库
2. **多数据库支持**：支持MySQL和PostgreSQL
3. **安全性检查**：禁止危险操作，只允许SELECT查询和WITH子句（CTE）
4. **高级SQL支持**：支持WITH子句、复杂子查询、JOIN、CROSS JOIN等
5. **归因分析**：提供详细的数据来源说明

## 输入参数

- `sql` (string, 必需): 要执行的SQL查询语句，支持SELECT查询和WITH子句（CTE）
- `max_rows` (number, 可选): 限制返回的最大行数，默认返回全部结果（最多1000行）
- `data_source_id` (string, 可选): 数据源ID，如果不提供则从Agent配置中获取

**支持的SQL类型**：
- ✅ SELECT查询
- ✅ WITH子句（CTE - Common Table Expression）
- ✅ 复杂子查询
- ✅ JOIN（包括CROSS JOIN）
- ✅ 聚合函数和窗口函数

## 输出格式

```json
{
  "success": true,
  "sql": "执行的SQL语句",
  "rowCount": 10,
  "rows": [...],
  "attribution": {
    "summary": "数据来源说明",
    "details": {
      "tables": ["table1", "table2"],
      "rowCount": 10,
      "columns": ["col1", "col2"],
      "hasWhere": true,
      "hasGroupBy": false,
      "hasOrderBy": true,
      "hasLimit": false
    },
    "guidance": [...]
  },
  "dataSource": {
    "id": "数据源ID",
    "name": "数据源名称",
    "type": "mysql",
    "database": "数据库名"
  }
}
```

## 执行流程

1. **获取数据源信息**
   - 从输入参数或Agent配置中获取 `data_source_id`
   - 查询数据源信息（host, port, database, username, password）
   - 解密密码

2. **安全性检查**
   - ✅ 验证SQL只包含SELECT查询或WITH子句（CTE）
   - ✅ 支持WITH子句结构：`WITH ... AS (SELECT ...)`
   - ✅ 使用精确的正则表达式检查危险操作，避免误判
   - ❌ 禁止写操作（INSERT, UPDATE, DELETE, DROP, CREATE TABLE等）

3. **创建数据库连接**
   - 根据数据源类型（MySQL/PostgreSQL）创建连接
   - 使用连接池管理连接

4. **执行SQL查询**
   - 执行SQL并获取结果
   - 限制返回行数（如果指定）

5. **归因分析**
   - 分析SQL结构（表、字段、子句）
   - 生成数据来源说明

## 数据源配置

数据源信息存储在MongoDB中，从前端业务列表选择的数据源获取：

1. **前端选择数据源**：用户在左侧业务列表中选择数据源
2. **数据源传递**：通过conversation.project_id或conversation.data_source_id传递
3. **自动连接**：工具自动从MongoDB获取数据源信息并连接数据库

数据源获取优先级：
1. 输入参数中的`data_source_id`
2. conversation.project_id → 项目 → 项目的data_source_id
3. conversation.data_source_id（前端直接传递）
4. req.body.data_source_id（请求体传递）

## 使用场景

- **智能问数**：根据用户自然语言查询生成SQL并执行
- **数据分析**：执行复杂的数据分析查询
- **报表生成**：生成数据报表

## 注意事项

- ✅ 支持SELECT查询和WITH子句（CTE），禁止写操作
- ✅ 支持复杂SQL查询（多个CTE、子查询、JOIN、CROSS JOIN等）
- ✅ 使用精确的安全检查，不会误判合法的只读查询
- 数据源密码是加密存储的，需要解密后才能使用
- 连接使用连接池管理，避免频繁创建连接
- 如果Agent未配置数据源，工具会返回错误

### WITH子句示例

**合法的WITH子句SQL**（✅ 可以正常执行）：
```sql
WITH oldest_female_clients AS (
    SELECT client_id, birth_date
    FROM client
    WHERE gender = 'F'
    ORDER BY birth_date ASC
    LIMIT 1
),
client_accounts AS (
    SELECT 
        ofc.client_id,
        d.A11 as avg_salary,
        a.account_id
    FROM oldest_female_clients ofc
    JOIN disp dp ON ofc.client_id = dp.client_id
    JOIN account a ON dp.account_id = a.account_id
    JOIN district d ON a.district_id = d.district_id
)
SELECT account_id, avg_salary
FROM client_accounts
ORDER BY avg_salary ASC
LIMIT 1
```

## 迁移说明

### 从旧版本迁移

**旧版本**（使用sql-api服务）：
- 通过环境变量配置数据库
- 调用独立的sql-api服务执行SQL
- 无法动态切换数据源

**新版本**（v2.0）：
- 从前端业务列表选择的数据源获取数据源信息
- 直接连接数据库执行SQL
- 支持动态切换数据源
- 不再依赖sql-api服务

### 兼容性

- 如果未提供数据源ID，工具会返回错误提示用户选择数据源
- 工具会自动从conversation或req.body中获取数据源信息

