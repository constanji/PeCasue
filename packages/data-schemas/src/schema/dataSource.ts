import { Schema } from 'mongoose';
import type { IDataSource } from '~/types';

const dataSourceSchema = new Schema<IDataSource>(
  {
    name: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    type: {
      type: String,
      required: true,
      enum: ['mysql', 'postgresql'],
      index: true,
    },
    host: {
      type: String,
      required: true,
      trim: true,
    },
    port: {
      type: Number,
      required: true,
      min: 1,
      max: 65535,
    },
    database: {
      type: String,
      required: true,
      trim: true,
    },
    username: {
      type: String,
      required: true,
      trim: true,
    },
    password: {
      type: String,
      required: true,
    },
    connectionPool: {
      min: {
        type: Number,
        default: 0,
        min: 0,
      },
      max: {
        type: Number,
        default: 10,
        min: 1,
      },
      idleTimeoutMillis: {
        type: Number,
        default: 30000,
        min: 1000,
      },
      connectionTimeoutMillis: {
        type: Number,
        default: 10000,
        min: 1000,
      },
    },
    ssl: {
      enabled: {
        type: Boolean,
        default: false,
      },
      rejectUnauthorized: {
        type: Boolean,
        default: true,
      },
      ca: {
        type: String,
        default: null,
      },
      cert: {
        type: String,
        default: null,
      },
      key: {
        type: String,
        default: null,
      },
    },
    status: {
      type: String,
      enum: ['active', 'inactive'],
      default: 'active',
      index: true,
    },
    isPublic: {
      type: Boolean,
      default: false,
      index: true,
    },
    lastTestedAt: {
      type: Date,
    },
    lastTestResult: {
      type: String,
      enum: ['success', 'failed'],
    },
    lastTestError: {
      type: String,
    },
    createdBy: {
      type: Schema.Types.ObjectId,
      ref: 'User',
      required: true,
      index: true,
    },
  },
  {
    timestamps: true,
  },
);

// 创建索引
dataSourceSchema.index({ name: 1, createdBy: 1 }, { unique: true });
dataSourceSchema.index({ type: 1, status: 1 });

export default dataSourceSchema;

