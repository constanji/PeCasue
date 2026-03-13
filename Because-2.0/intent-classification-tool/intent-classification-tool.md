---
name: intent-classification-tool
description: 分类用户查询意图（TEXT_TO_SQL / GENERAL / MISLEADING_QUERY），使用RAG检索提高分类准确性
category: intent-classification
version: 1.0
---

# Intent Classification Tool

## 概述

智能意图分类工具，根据用户查询和RAG检索的知识，将用户查询分类为三种意图之一：

- **TEXT_TO_SQL**: 查询与数据库模式相关，需要生成SQL
- **GENERAL**: 查询关于数据库模式的一般信息
- **MISLEADING_QUERY**: 查询与数据库模式无关或缺乏详细信息

## 核心能力

1. **LLM优先判断**：首先使用LLM进行意图判断，快速高效
2. **RAG辅助增强**：当LLM无法判断或缺少依据时，使用RAG检索语义模型、QA对、业务知识进行辅助判断
3. **轻量级设计**：只返回分类结果，不包含冗长的模板说明
4. **上下文感知**：结合查询历史和上下文信息进行意图判断

## 输入参数

- `query` (string, 必需): 用户查询文本
- `use_rag` (boolean, 可选): 是否在LLM无法判断时使用RAG检索作为辅助，默认 true
- `top_k` (number, 可选): RAG检索返回数量（仅在启用RAG时使用），默认 5

## 输出格式

```json
{
  "intent": "TEXT_TO_SQL" | "GENERAL" | "MISLEADING_QUERY",
  "confidence": 0.0-1.0,
  "reasoning": "简短推理说明（最多20词）",
  "rephrased_question": "重述后的完整问题（如有）",
  "rag_context": {
    "semantic_models_found": boolean,
    "qa_pairs_found": boolean,
    "business_knowledge_found": boolean
  }
}
```

## 执行流程

1. **LLM意图判断**（优先）
   - 首先使用LLM基于查询文本进行意图判断
   - LLM分析查询内容，判断属于三种意图中的哪一种
   - 如果LLM能够明确判断且置信度较高，直接返回结果

2. **RAG知识检索**（辅助判断，仅在需要时）
   - 当LLM无法明确判断或缺少判断依据时，启用RAG检索
   - 调用 `/api/rag/query` 检索语义模型、QA对、业务知识
   - 提取检索结果，判断查询与数据库的相关性
   - 如果检索到相关语义模型，更可能是 TEXT_TO_SQL
   - 参考QA对中类似问题的处理方式

3. **综合判断**
   - 结合LLM判断结果和RAG检索结果进行综合判断
   - 如果LLM判断明确，优先采用LLM结果
   - 如果LLM判断不确定，使用RAG结果进行辅助判断

4. **返回分类结果**
   - 返回意图类型、置信度、推理说明
   - 包含RAG上下文信息（如果使用了RAG）

## 意图分类规则

### TEXT_TO_SQL

- 用户的输入与数据库模式相关，需要SQL查询
- 问题包含对特定表、列或数据详情的引用
- RAG检索到相关语义模型

### GENERAL

- 用户寻求关于数据库模式或其整体功能的一般信息
- 查询没有提供足够的细节来生成特定的SQL查询

### MISLEADING_QUERY

- 用户的输入与数据库模式无关
- 用户的输入缺乏生成SQL查询所需的特定细节
- RAG检索未找到相关语义模型

## 注意事项

- **LLM优先原则**：首先使用LLM进行意图判断，只有在LLM无法判断或缺少依据时才使用RAG辅助
- **RAG辅助原则**：RAG检索作为辅助手段，用于增强判断准确性，而非主要判断方式
- 推理说明必须清晰、简洁，限制在20个词以内
- 重述问题和推理必须使用与用户输出语言相同的语言
- 如果有查询历史，必须结合上下文理解当前查询
