---
name: rag-retrieval-tool
description: 从知识库中检索语义模型、QA对、同义词、业务知识等多源知识，支持向量检索和混合检索
category: rag-retrieval
version: 1.0
---

# RAG Retrieval Tool

## 概述

RAG知识检索工具，从知识库中检索与查询相关的多源知识：
- **语义模型** (semantic_model): 数据库表结构信息
- **QA对** (qa_pair): 类似问题的SQL示例和答案
- **同义词** (synonym): 业务术语映射
- **业务知识** (business_knowledge): 业务规则和文档

## 核心能力

1. **多源知识检索**：一次调用检索多种类型的知识
2. **向量检索**：使用embedding进行语义相似度搜索
3. **混合检索**：结合关键词和向量检索提高召回率
4. **结果格式化**：返回结构化的检索结果，包含相似度分数

## 输入参数

- `query` (string, 必需): 查询文本
- `types` (array, 可选): 要检索的知识类型，默认全部类型
  - 可选值: `semantic_model`, `qa_pair`, `synonym`, `business_knowledge`
- `top_k` (number, 可选): 返回数量，默认 10
- `use_reranking` (boolean, 可选): 是否使用重排序，默认 true
- `enhanced_reranking` (boolean, 可选): 是否使用增强重排序，默认 false
- `entity_id` (string, 可选): 实体ID过滤
- `file_ids` (array, 可选): 文件ID数组过滤

## 输出格式

```json
{
  "query": "原始查询文本",
  "results": [
    {
      "rank": 1,
      "type": "semantic_model" | "qa_pair" | "synonym" | "business_knowledge",
      "title": "标题",
      "content": "内容",
      "score": 0.0-1.0,
      "metadata": {
        // 类型特定的元数据
      }
    }
  ],
  "total": 10,
  "metadata": {
    "retrieval_count": 20,
    "reranked": true,
    "enhanced_reranking": false
  }
}
```

## 执行流程

1. **向量化查询**
   - 将查询文本转换为embedding向量

2. **向量检索**
   - 在向量数据库中搜索相似的知识条目
   - 支持按类型过滤（semantic_model, qa_pair等）

3. **重排序**（可选）
   - 使用reranker模型对检索结果进行重排序
   - 提高相关性，确保最相关的知识排在前面

4. **格式化结果**
   - 按类型组织检索结果
   - 添加相似度分数和元数据

## 使用场景

- **SQL生成前**：检索相关语义模型和QA对，帮助生成准确的SQL
- **意图分类**：检索语义模型判断查询是否与数据库相关
- **业务理解**：检索业务知识和同义词，理解业务术语

## 注意事项

- 检索结果按相似度分数降序排列
- 如果启用重排序，会先检索更多结果（topK * 2），然后重排序返回topK个
- 不同类型的知识有不同的元数据结构

