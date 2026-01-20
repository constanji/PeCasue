import mongoose, { Schema } from 'mongoose';

/**
 * 知识库条目类型
 */
export const KnowledgeType = {
  SEMANTIC_MODEL: 'semantic_model', // 语义模型
  QA_PAIR: 'qa_pair', // QA对
  SYNONYM: 'synonym', // 同义词
  BUSINESS_KNOWLEDGE: 'business_knowledge', // 业务知识
  FILE: 'file', // 文件（兼容现有文件向量化）
};

/**
 * 知识库条目类型（TypeScript 类型）
 */
export type KnowledgeTypeValue = typeof KnowledgeType[keyof typeof KnowledgeType];

/**
 * 知识库条目接口
 */
export interface IKnowledgeEntry extends Document {
  user?: mongoose.Types.ObjectId; // 改为可选，支持Agent共享知识库
  type: KnowledgeTypeValue;
  title: string;
  content: string;
  embedding?: number[]; // 向量嵌入
  parent_id?: mongoose.Types.ObjectId; // 父级知识条目ID（用于层级结构，如数据库-表的关系）
  metadata?: {
    // 语义模型相关
    semantic_model_id?: string;
    database_name?: string;
    table_name?: string;
    is_database_level?: boolean; // 是否为数据库级别的语义模型
    
    // QA对相关
    question?: string;
    answer?: string;
    
    // 同义词相关
    noun?: string;
    synonyms?: string[];
    
    // 业务知识相关
    category?: string;
    tags?: string[];
    
    // 文件相关（兼容现有）
    file_id?: string;
    filename?: string;
    page?: number;
    chunk_index?: number;
    
    // 通用
    source?: string;
    entity_id?: string;
    [key: string]: any;
  };
  createdAt?: Date;
  updatedAt?: Date;
}

const knowledgeEntrySchema: Schema<IKnowledgeEntry> = new Schema(
  {
    user: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'User',
      index: true,
      required: false, // 改为可选，支持Agent共享知识库
    },
    type: {
      type: String,
      enum: Object.values(KnowledgeType),
      required: true,
      index: true,
    },
    title: {
      type: String,
      required: true,
    },
    content: {
      type: String,
      required: true,
    },
    embedding: {
      type: [Number],
      default: undefined,
    },
    parent_id: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'KnowledgeEntry',
      default: null,
      index: true,
    },
    metadata: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },
  },
  {
    timestamps: true,
  },
);

// 创建索引
knowledgeEntrySchema.index({ user: 1, type: 1 });
knowledgeEntrySchema.index({ 'metadata.file_id': 1 });
knowledgeEntrySchema.index({ 'metadata.semantic_model_id': 1 });
knowledgeEntrySchema.index({ 'metadata.entity_id': 1 });
// Note: parent_id index is defined in the field schema above
knowledgeEntrySchema.index({ createdAt: -1 });

export default knowledgeEntrySchema;

