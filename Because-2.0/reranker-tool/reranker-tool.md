---
name: reranker-tool
description: 对检索结果进行重排序优化，使用reranker模型提高相关性，支持增强重排序
category: reranking
version: 1.0
---

# Reranker Tool

## 概述

重排序工具，对检索结果进行重新排序以提高相关性。支持：
- **基础重排序**：使用reranker模型对结果重新评分
- **增强重排序**：结合相似度、类型权重、时效性等多因素重排序

## 核心能力

1. **模型级重排序**：使用ONNX或外部reranker模型进行重排序
2. **增强重排序**：结合多种因素计算综合分数
3. **灵活配置**：支持自定义权重和topK参数

## 输入参数

- `query` (string, 必需): 原始查询文本
- `results` (array, 必需): 检索结果数组，每个结果应包含 `content` 或 `text` 字段
- `top_k` (number, 可选): 返回前K个结果，默认 10
- `enhanced` (boolean, 可选): 是否使用增强重排序，默认 false
- `weights` (object, 可选): 增强重排序的权重配置
  - `similarity_weight` (number): 相似度权重，默认 0.7
  - `type_weight` (number): 类型权重，默认 0.2
  - `recency_weight` (number): 时效性权重，默认 0.1

## 输出格式

```json
{
  "query": "原始查询文本",
  "reranked_results": [
    {
      "rank": 1,
      "content": "内容",
      "score": 0.0-1.0,
      "reranked": true,
      "enhanced": false
    }
  ],
  "total": 10,
  "metadata": {
    "reranker_type": "onnx" | "external" | "default",
    "enhanced": false,
    "original_count": 20
  }
}
```

## 执行流程

1. **提取文档文本**
   - 从检索结果中提取 `content` 或 `text` 字段

2. **调用重排序器**
   - 如果启用增强重排序：先基础重排序，再计算增强分数
   - 否则：直接使用reranker模型重排序

3. **映射结果**
   - 将重排序结果映射回原始结果结构
   - 保留原始元数据，更新分数和排名

4. **返回结果**
   - 返回重排序后的结果数组
   - 包含重排序元数据

## 使用场景

- **RAG检索后**：对RAG检索结果进行重排序，提高相关性
- **多源检索**：合并多个检索源的结果后进行统一重排序
- **结果优化**：在返回给用户前优化结果顺序

## 注意事项

- 重排序会改变结果的顺序和分数
- 增强重排序会考虑类型权重（语义模型和QA对优先级更高）
- 如果reranker模型不可用，会使用默认排序（按分数降序）

