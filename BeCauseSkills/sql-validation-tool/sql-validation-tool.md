---
name: sql-validation-tool
description: SQL合法性/安全性检测工具，检查SQL语法、安全性、表字段存在性等
category: sql-validation
version: 1.1
---

# SQL Validation Tool

## 概述

SQL校验工具，对SQL查询进行合法性、安全性和正确性检查：
- **安全性检查**：禁止DROP、DELETE、UPDATE等危险操作
- **语法检查**：验证SQL语法是否正确
- **表字段检查**：验证表和字段是否存在（如果提供schema）
- **权限检查**：检查是否越权访问
- **WITH子句支持**：完整支持WITH子句（CTE）的识别和验证

1. **安全性验证**：禁止写操作和DDL操作
2. **语法验证**：基本SQL语法检查
3. **Schema验证**：验证表和字段是否存在（可选）
4. **风险评估**：评估SQL的风险等级

## 输入参数

- `sql` (string, 必需): 要校验的SQL查询语句
- `check_schema` (boolean, 可选): 是否检查表和字段是否存在，默认 false
- `schema_info` (object, 可选): 数据库schema信息（如果check_schema为true）
  - `tables` (array): 表名数组
  - `fields` (object): 字段映射，格式为 `{ tableName: [field1, field2] }`

## 输出格式

```json
{
  "valid": true | false,
  "errors": [
    {
      "type": "security" | "syntax" | "schema" | "permission",
      "message": "错误信息",
      "severity": "error"
    }
  ],
  "warnings": [
    {
      "type": "performance" | "best_practice" | "syntax",
      "message": "警告信息"
    }
  ],
  "risk_level": "low" | "medium" | "high",
  "metadata": {
    "tables_used": ["table1", "table2"],
    "operations": ["SELECT"] | ["WITH", "SELECT"],
    "has_with_clause": true | false,
    "has_join": false,
    "has_subquery": false,
    "has_group_by": false,
    "has_order_by": false,
    "has_limit": false
  }
}
```

**重要说明**：
- `valid: false` 仅当存在 `errors` 时（写操作、语法错误等）
- `warnings` **不会**导致 `valid: false`，只是提醒注意性能或最佳实践
- 合法的只读SQL（即使包含CROSS JOIN、复杂CTE等）会返回 `valid: true`，可能带有 `warnings`

## 执行流程

1. **安全性检查**
   - ✅ 检查SQL是否以SELECT或WITH开头（已支持WITH子句）
   - ✅ 验证WITH子句结构：`WITH ... AS (SELECT ...)`
   - ✅ 使用精确的正则表达式检查危险关键词（DROP、DELETE、UPDATE等）
   - ✅ 确保只允许SELECT查询和WITH子句

2. **语法检查**
   - 基本SQL语法验证
   - 检查括号匹配、引号匹配等
   - 验证WITH子句格式

3. **Schema检查**（如果启用）
   - 验证表是否存在
   - 验证字段是否存在

4. **风险评估**
   - 根据检查结果评估风险等级
   - 生成警告和建议

## 校验器设计原则

**核心原则**：只检查**写操作**，不因查询复杂度拒绝合法的只读SQL。

### ✅ 校验器会放行的SQL
- ✅ 所有合法的SELECT查询（无论多复杂）
- ✅ WITH子句（CTE），包括多个CTE
- ✅ CROSS JOIN（会产生性能警告，但不会拒绝）
- ✅ 复杂的子查询和聚合
- ✅ 窗口函数和高级SQL特性

### ❌ 校验器会拒绝的SQL
- ❌ 任何写操作（INSERT、UPDATE、DELETE）
- ❌ DDL操作（CREATE、DROP、ALTER）
- ❌ 权限操作（GRANT、REVOKE）

### ⚠️ 校验器会警告但不拒绝的SQL
- ⚠️ CROSS JOIN（性能警告）
- ⚠️ SELECT *（最佳实践警告）
- ⚠️ 复杂查询（性能警告）

**重要**：如果您的SQL是纯只读查询，即使很复杂，也不会被拒绝。只有真正的写操作才会被阻止。

## 安全检查规则

### 禁止的操作
- DROP TABLE / DATABASE
- DELETE FROM
- UPDATE SET
- INSERT INTO
- ALTER TABLE
- TRUNCATE TABLE
- CREATE TABLE / DATABASE
- GRANT / REVOKE

### 允许的操作
- SELECT（只读查询）
- WITH子句（CTE - Common Table Expression）✅ 已支持
- JOIN（关联查询，包括CROSS JOIN）✅ 允许但会有性能警告
- 子查询
- 聚合函数（COUNT、SUM等）
- 窗口函数

### ⚠️ 性能警告（非错误）

以下操作是**合法的只读操作**，但会产生性能警告：

- **CROSS JOIN**：可能产生笛卡尔积，会产生性能警告但不会阻止执行
- **SELECT ***：建议明确指定字段，会产生最佳实践警告
- **复杂查询**：多个CTE或子查询，会产生性能警告

**重要**：性能警告不会导致SQL被拒绝，只是提醒注意性能影响。

## 使用场景

- **SQL生成后**：在SQL执行前进行校验
- **用户输入验证**：验证用户直接输入的SQL
- **安全审计**：检查SQL是否符合安全策略

## 注意事项

- 默认只进行安全性和基本语法检查
- Schema检查需要提供数据库schema信息
- 风险评估基于检查结果，仅供参考

### WITH子句和CROSS JOIN示例

**合法的WITH子句SQL**（✅ 会通过校验，可能有性能警告）：
```sql
WITH loan_agg AS (
    SELECT account_id, SUM(amount) AS total_loan_amount
    FROM loan
    GROUP BY account_id
)
SELECT account_id, total_loan_amount
FROM loan_agg
WHERE total_loan_amount > 100000
```

**包含CROSS JOIN的复杂SQL**（✅ 会通过校验，会有性能警告）：
```sql
WITH oldest_female_clients AS (
    SELECT client_id
    FROM client
    WHERE gender = 'F'
    ORDER BY birth_date ASC
    LIMIT 1
),
client_account_info AS (
    SELECT 
        c.client_id,
        d.A11 AS avg_salary
    FROM oldest_female_clients c
    JOIN disp dp ON c.client_id = dp.client_id AND dp.type = 'OWNER'
    JOIN account a ON dp.account_id = a.account_id
    JOIN district d ON a.district_id = d.district_id
),
salary_range AS (
    SELECT 
        MAX(A11) AS max_salary,
        MIN(A11) AS min_salary
    FROM district
)
SELECT 
    cai.client_id,
    cai.avg_salary AS lowest_avg_salary,
    sr.max_salary,
    sr.min_salary,
    (sr.max_salary - sr.min_salary) AS salary_difference
FROM client_account_info cai
CROSS JOIN salary_range sr
ORDER BY cai.avg_salary ASC
LIMIT 1
```

**校验结果**：
```json
{
  "valid": true,
  "warnings": [
    {
      "type": "performance",
      "message": "检测到 CROSS JOIN，可能产生笛卡尔积。请确认这是预期的行为，并注意性能影响。"
    },
    {
      "type": "performance",
      "message": "查询包含多个CTE或子查询，可能影响性能。请确认查询逻辑正确。"
    }
  ],
  "risk_level": "low",
  "metadata": {
    "tables_used": ["client", "disp", "account", "district"],
    "operations": ["WITH", "SELECT"],
    "has_with_clause": true,
    "has_join": true,
    "has_subquery": true
  }
}
```


