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

def generate_agent_comment_prompt(question, sql_dialect, knowledge=None):
    """为智能体生成问题提示，智能体可以使用工具来完成任务"""
    base_prompt = f"请使用有效的 {sql_dialect} SQL"
    knowledge_text = " 并理解外部知识" if knowledge else ""
    knowledge_prompt = f"\n外部知识: {knowledge}" if knowledge else ""

    combined_prompt = (
        f"{base_prompt}{knowledge_text}，回答以下关于上述表的问题。\n"
        f"问题: {question}\n"
        f"{knowledge_prompt}"
    )
    return combined_prompt


def generate_agent_instruction_prompt(sql_dialect):
    """为智能体生成指令提示，指导智能体如何使用工具完成任务"""
    return f"""
请使用可用的工具（如 database_schema、text-to-sql、sql_executor 等）来完成以下任务：
1. 首先使用 database_schema 工具获取数据库表结构信息
2. 然后使用 text-to-sql 工具将自然语言问题转换为 {sql_dialect} SQL 查询
3. 如果需要，可以使用 sql_executor 工具执行 SQL 并查看结果

最终请返回生成的 {sql_dialect} SQL 代码，以 SELECT 开头。
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
