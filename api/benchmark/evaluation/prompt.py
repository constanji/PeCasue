from table_schema import generate_schema_prompt


def generate_comment_prompt(question, sql_dialect, knowledge=None):
    base_prompt = f"-- Using valid {sql_dialect}"
    knowledge_text = " and understanding External Knowledge" if knowledge else ""
    knowledge_prompt = f"-- External Knowledge: {knowledge}" if knowledge else ""

    combined_prompt = (
        f"{base_prompt}{knowledge_text}, answer the following questions for the tables provided above.\n"
        f"-- {question}\n"
        f"{knowledge_prompt}"
    )
    return combined_prompt


def generate_cot_prompt(sql_dialect):
    return f"\nGenerate the {sql_dialect} for the above question after thinking step by step: "


def generate_instruction_prompt(sql_dialect):
    return f"""
        \nIn your response, you do not need to mention your intermediate steps. 
        Do not include any comments in your response.
        Do not need to start with the symbol ```
        You only need to return the result {sql_dialect} SQL code
        start from SELECT
        """


def generate_combined_prompts_one(db_path, question, sql_dialect, knowledge=None):
    schema_prompt = generate_schema_prompt(sql_dialect, db_path)
    comment_prompt = generate_comment_prompt(question, sql_dialect, knowledge)
    cot_prompt = generate_cot_prompt(sql_dialect)
    instruction_prompt = generate_instruction_prompt(sql_dialect)

    combined_prompts = "\n\n".join(
        [schema_prompt, comment_prompt, cot_prompt, instruction_prompt]
    )
    return combined_prompts


# ========== Agent提示词生成函数 ==========

SQLITE_DIALECT_GUIDE = """
- 使用 SQLite 语法，不要使用 MySQL/PostgreSQL 特有函数
- 日期提取: 使用 STRFTIME('%Y', date_col) 而非 DATE_FORMAT() 或 YEAR()
- 类型转换: 使用 CAST(x AS REAL) 而非 CAST(x AS DOUBLE)
- 字符串: 使用单引号 'value'，表名/列名不要加反引号
- 布尔判断: SUM(col = 'value') 直接使用，SQLite 中比较返回 0/1
- 不支持: DATE_FORMAT, YEAR(), MONTH(), TIMESTAMPDIFF, CURDATE()
- 可用: STRFTIME, DATE, TIME, JULIANDAY, || (字符串拼接)
""".strip()


def generate_agent_comment_prompt(question, sql_dialect, knowledge=None):
    """为智能体生成问题提示"""
    knowledge_prompt = f"\n**外部知识**: {knowledge}" if knowledge else ""

    combined_prompt = (
        f"请仔细阅读上方的数据库表结构（CREATE TABLE 语句），根据表结构中的**表名、列名、数据类型和外键关系**，"
        f"为以下问题生成正确的 **SQLite** SQL 查询。\n\n"
        f"**问题**: {question}\n"
        f"{knowledge_prompt}"
    )
    return combined_prompt


JOIN_GRAPH = """
# Join Graph（表关联最短路径）

```
district ─── client        （client.district_id = district.district_id）
district ─── account       （account.district_id = district.district_id）
client ───── disp ───── account  （disp 是 client↔account 的桥表）
account ──── loan          （loan.account_id = account.account_id）
account ──── trans         （trans.account_id = account.account_id）
account ──── order         （order.account_id = account.account_id）
disp ─────── card          （card.disp_id = disp.disp_id）
```

⚠️ **JOIN 规则**：
- client + district 属性 → `client JOIN district`，**不要** 经过 disp/account
- account + district 属性 → `account JOIN district`，**不要** 经过 client/disp
- client + account 属性 → **必须经过 disp 桥表**
- 只 JOIN 问题**真正需要**的表，不要多余 JOIN
""".strip()

ENUM_MAPPING = """
# 枚举值映射（自然语言 → 数据库实际值）

数据库使用捷克语枚举值，查询时**必须使用实际值**，不能用英文：
- credit card withdrawal → `'VYBER KARTOU'`
- cash withdrawal → `'VYBER'`
- cash deposit → `'VKLAD'`
- collection from another bank → `'PREVOD Z UCTU'`
- remittance to another bank → `'PREVOD NA UCET'`
- insurance payment → `'POJISTNE'`
- household payment → `'SIPO'`
- loan payment → `'UVER'`
- monthly issuance → `'POPLATEK MESICNE'`
- permanent order（定期转账指令） → 查 `order` 表
- transaction（实际交易） → 查 `trans` 表
""".strip()

SQL_RULES = """
# SQL 生成规则

1. **只返回问题要求的字段** — 不要附带额外列
2. **使用最短 JOIN 路径** — 参考上方 Join Graph
3. **优先简洁写法** — 找 MAX/MIN 用 `ORDER BY ... LIMIT 1`，不用子查询
4. **避免不必要的 CTE** — 除非确实需要多步聚合
5. **使用实际枚举值** — 参考上方映射表
6. **不要执行 SQL** — 只需要生成
""".strip()


def generate_agent_instruction_prompt(sql_dialect):
    """为智能体生成指令提示"""
    return f"""
# SQL 方言要求（SQLite）
{SQLITE_DIALECT_GUIDE}

{JOIN_GRAPH}

{ENUM_MAPPING}

{SQL_RULES}

# Think step by step

**Step 1 — Identify required tables**
从问题中提取关键实体，确定需要查询哪些表。只选择**真正需要**的表。

**Step 2 — Identify join keys**
根据 Join Graph 确定表之间的关联键。选择**最短路径**，不要多余 JOIN。

**Step 3 — Identify filter conditions**
确定 WHERE / HAVING / GROUP BY / ORDER BY / LIMIT 和聚合函数（COUNT/SUM/AVG/MAX/MIN）。
注意使用数据库中的**实际列名和枚举值**。

**Step 4 — Construct SQL**
基于以上分析，构造简洁、正确的 SQLite SQL。

# 可用工具

- **database-schema**：获取表结构详情
- **sql-validation**：校验 SQL 语法

【输出格式】最终回复中必须包含一个 ```sql 代码块：
```sql
SELECT ...
```
"""


def generate_agent_combined_prompts_one(db_path, question, sql_dialect, knowledge=None):
    """为智能体生成完整的提示词组合"""
    schema_prompt = generate_schema_prompt(sql_dialect, db_path)
    comment_prompt = generate_agent_comment_prompt(question, sql_dialect, knowledge)
    instruction_prompt = generate_agent_instruction_prompt(sql_dialect)

    combined_prompts = "\n\n".join(
        [schema_prompt, comment_prompt, instruction_prompt]
    )
    return combined_prompts
