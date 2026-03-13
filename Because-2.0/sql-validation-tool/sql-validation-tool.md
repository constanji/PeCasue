# SQL 校验工具 (SQL Validation Tool)

## 概述

SQL合法性/安全性/质量检测工具，在原有功能基础上新增：
- **7类关键字分类体系**：对SQL进行结构化分类分析
- **双盲SQL对比验证**：支持两个独立标注者的SQL一致性检验
- **文本-知识-SQL对齐检查**：确保SQL与用户问题和外部知识对齐
- **SQL复杂度评估**：基于关键字分类的量化复杂度评分

## 7类SQL关键字分类

| 类别 | 名称 | 关键字 |
|------|------|--------|
| primary | 主体关键字 | SELECT, FROM, WHERE, AND, OR, NOT, IN, EXISTS, IS, NULL, IIF, CASE, CASE WHEN |
| join | JOIN关键字 | INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN, CROSS JOIN, ON, AS |
| clause | 子句关键字 | BETWEEN, LIKE, LIMIT, ORDER BY, ASC, DESC, GROUP BY, HAVING, UNION, ALL, EXCEPT, PARTITION BY |
| aggregate | 聚合函数 | AVG, COUNT, MAX, MIN, ROUND, SUM |
| scalar | 标量函数 | ABS, LENGTH, STRFTIME, JULIANDAY, NOW, CAST, SUBSTR, INSTR |
| comparison | 比较关键字 | =, >, <, >=, <=, != |
| arithmetic | 计算关键字 | -, +, *, / |

## 双盲测试方法

1. 两个独立的SQL标注者为同一问题生成SQL
2. 工具对比两个SQL的：
   - **结构相似度**：WHERE/JOIN/GROUP BY等结构特征对比
   - **关键字使用**：7类关键字的Jaccard相似度
   - **执行结果**：结果行数、列结构、值匹配率
3. 如果结果匹配率 ≥ 95%，视为等价（consensus: agreed）
4. 否则标记为 disagreed，需要专家审查
5. 工具自动推荐更高效的SQL版本（复杂度更低的）

## 测试维度

### (1) SQL可执行性
- 结构完整性检查（SELECT/FROM）
- 括号/引号匹配
- 安全性检查（禁止写操作）

### (2) 文本-知识-SQL对齐
- question → SQL 的词汇覆盖度
- evidence → SQL 的术语匹配度
- question → evidence 的语义关联度

## 输入参数

```json
{
  "sql": "SELECT region, SUM(revenue) FROM sales GROUP BY region",
  "compare_sql": "SELECT region, SUM(amount) AS revenue FROM sales GROUP BY region",
  "compare_results": {
    "sql1_results": [...],
    "sql2_results": [...]
  },
  "question": "各地区的销售收入是多少？",
  "evidence": "revenue字段代表销售收入，sales表存储销售数据"
}
```

## 输出结构

```json
{
  "valid": true,
  "risk_level": "low",
  "complexity": {
    "score": 12,
    "level": "moderate",
    "factors": ["主体关键字: 3个", "聚合函数: SUM", "子句: GROUP BY"]
  },
  "keyword_classification": {
    "primary": { "matched": 3, "keywords": ["SELECT", "FROM", "WHERE"] },
    "aggregate": { "matched": 1, "keywords": ["SUM"] },
    ...
  },
  "double_blind_comparison": {
    "structural": { "similarity": 0.85 },
    "keyword": { ... },
    "result": { "isEquivalent": true, "matchRate": 1.0 },
    "consensus": "agreed",
    "efficiencyComparison": { "moreEfficient": "sql1" }
  },
  "alignment": {
    "executability": { "likely_executable": true },
    "overallAlignment": 0.75
  }
}
```
