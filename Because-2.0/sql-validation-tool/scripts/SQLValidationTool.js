const { Tool } = require('@langchain/core/tools');
const { z } = require('zod');
const { logger } = require('@because/data-schemas');

/**
 * SQL关键字分类体系（7类）
 * 用于SQL复杂度评估、测试覆盖度分析和双盲标注质量检验
 */
const SQL_KEYWORD_CATEGORIES = {
  primary: {
    name: '主体关键字',
    keywords: ['SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'IS', 'NULL', 'IIF', 'CASE', 'CASE WHEN'],
  },
  join: {
    name: 'JOIN关键字',
    keywords: ['INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN', 'CROSS JOIN', 'ON', 'AS'],
  },
  clause: {
    name: '子句关键字',
    keywords: ['BETWEEN', 'LIKE', 'LIMIT', 'ORDER BY', 'ASC', 'DESC', 'GROUP BY', 'HAVING', 'UNION', 'ALL', 'EXCEPT', 'PARTITION BY'],
  },
  aggregate: {
    name: '聚合函数关键字',
    keywords: ['AVG', 'COUNT', 'MAX', 'MIN', 'ROUND', 'SUM'],
  },
  scalar: {
    name: '标量关键字',
    keywords: ['ABS', 'LENGTH', 'STRFTIME', 'JULIANDAY', 'NOW', 'CAST', 'SUBSTR', 'INSTR'],
  },
  comparison: {
    name: '比较关键字',
    keywords: ['=', '>', '<', '>=', '<=', '!=', '<>'],
  },
  arithmetic: {
    name: '计算关键字',
    keywords: ['-', '+', '*', '/'],
  },
};

/**
 * SQL Validation Tool - SQL校验工具（增强版）
 * 
 * 功能增强：
 * 1. 7类关键字分类和复杂度评估
 * 2. 双盲SQL标注对比测试
 * 3. SQL可执行性预检
 * 4. 文本-知识-SQL对齐检查
 */
class SQLValidationTool extends Tool {
  name = 'sql_validation';

  description =
    'SQL验证工具（增强版）：检查SQL语法、安全性、表字段存在性。' +
    '支持7类关键字分类分析、SQL复杂度评估、双盲SQL对比验证。' +
    '禁止写操作，只允许SELECT查询和WITH子句（CTE）。';

  schema = z.object({
    sql: z
      .string()
      .min(1)
      .describe('要校验的SQL查询语句'),
    check_schema: z
      .boolean()
      .optional()
      .default(false)
      .describe('是否检查表和字段是否存在，默认false'),
    schema_info: z
      .object({
        tables: z.array(z.string()).optional(),
        fields: z.record(z.array(z.string())).optional(),
      })
      .optional()
      .describe('数据库schema信息（如果check_schema为true）'),
    compare_sql: z
      .string()
      .optional()
      .describe('双盲对比SQL（第二个标注者的SQL），用于一致性检验'),
    compare_results: z
      .object({
        sql1_results: z.array(z.any()).optional(),
        sql2_results: z.array(z.any()).optional(),
      })
      .optional()
      .describe('双盲对比的执行结果'),
    evidence: z
      .string()
      .optional()
      .describe('外部知识/证据（evidence），用于文本-知识-SQL对齐检查'),
    question: z
      .string()
      .optional()
      .describe('原始用户问题，用于文本-知识-SQL对齐检查'),
  });

  // ==================== 7类关键字分类 ====================

  /**
   * 对SQL进行7类关键字分类分析
   * @param {string} sql - SQL语句
   * @returns {Object} 分类结果
   */
  classifyKeywords(sql) {
    const upper = sql.toUpperCase();
    const result = {};
    let totalMatched = 0;

    for (const [category, config] of Object.entries(SQL_KEYWORD_CATEGORIES)) {
      const matched = [];

      for (const keyword of config.keywords) {
        // 对于多词关键字（如 INNER JOIN），直接匹配
        // 对于单字符操作符（如 =, >, <），使用特殊处理避免误匹配
        if (keyword.length <= 2 && /^[=><!=+\-*/]+$/.test(keyword)) {
          if (this._matchOperator(sql, keyword)) {
            matched.push(keyword);
          }
        } else {
          const pattern = new RegExp(`\\b${keyword.replace(/\s+/g, '\\s+')}\\b`, 'i');
          if (pattern.test(upper)) {
            matched.push(keyword);
          }
        }
      }

      result[category] = {
        name: config.name,
        total: config.keywords.length,
        matched: matched.length,
        keywords: matched,
        coverage: matched.length / config.keywords.length,
      };

      totalMatched += matched.length;
    }

    result._summary = {
      totalKeywordsMatched: totalMatched,
      categoriesUsed: Object.values(result).filter((v) => v.matched > 0).length - 1,
      totalCategories: 7,
    };

    return result;
  }

  /**
   * 匹配SQL操作符，避免将 >= 中的 > 或 = 单独匹配
   */
  _matchOperator(sql, operator) {
    const opMap = {
      '>=': /(?<!=)>=(?!=)/,
      '<=': /(?<!=)<=(?!=)/,
      '!=': /!=(?!=)/,
      '<>': /<>/,
      '>': /(?<!=)>(?!=)/,
      '<': /(?<!=|!)<(?!=|>)/,
      '=': /(?<!<|>|!)=(?!=)/,
      '+': /\+/,
      '-': /(?<!-)-(?!-|>)/,
      '*': /\*(?!\*)/,
      '/': /(?<!\/)\/(?!\/|\*)/,
    };

    const pattern = opMap[operator];
    if (pattern) {
      return pattern.test(sql);
    }
    return sql.includes(operator);
  }

  /**
   * 评估SQL复杂度（基于关键字分类）
   */
  assessComplexity(sql) {
    const keywords = this.classifyKeywords(sql);

    let score = 0;
    const factors = [];

    // 主体关键字基础分
    score += keywords.primary.matched * 1;
    if (keywords.primary.matched > 0) {
      factors.push(`主体关键字: ${keywords.primary.matched}个`);
    }

    // JOIN增加复杂度
    score += keywords.join.matched * 3;
    if (keywords.join.matched > 0) {
      factors.push(`JOIN操作: ${keywords.join.keywords.join(', ')}`);
    }

    // 子句增加复杂度
    score += keywords.clause.matched * 2;
    if (keywords.clause.matched > 0) {
      factors.push(`子句: ${keywords.clause.keywords.join(', ')}`);
    }

    // 聚合函数
    score += keywords.aggregate.matched * 2;
    if (keywords.aggregate.matched > 0) {
      factors.push(`聚合函数: ${keywords.aggregate.keywords.join(', ')}`);
    }

    // 标量函数
    score += keywords.scalar.matched * 2;
    if (keywords.scalar.matched > 0) {
      factors.push(`标量函数: ${keywords.scalar.keywords.join(', ')}`);
    }

    // 子查询嵌套
    const subqueryCount = (sql.match(/\(\s*SELECT\b/gi) || []).length;
    score += subqueryCount * 5;
    if (subqueryCount > 0) {
      factors.push(`子查询嵌套: ${subqueryCount}层`);
    }

    // WITH/CTE
    const cteCount = (sql.match(/\bWITH\s+\w+\s+AS\s*\(/gi) || []).length;
    score += cteCount * 4;
    if (cteCount > 0) {
      factors.push(`CTE: ${cteCount}个`);
    }

    let level;
    if (score <= 5) {
      level = 'simple';
    } else if (score <= 15) {
      level = 'moderate';
    } else if (score <= 30) {
      level = 'complex';
    } else {
      level = 'very_complex';
    }

    return {
      score,
      level,
      factors,
      keywordCategories: keywords,
    };
  }

  // ==================== 双盲SQL对比 ====================

  /**
   * 双盲SQL对比验证
   * 两个独立标注者的SQL结果一致性检查
   * 
   * @param {string} sql1 - 标注者1的SQL
   * @param {string} sql2 - 标注者2的SQL
   * @param {Object[]} results1 - SQL1的执行结果
   * @param {Object[]} results2 - SQL2的执行结果
   * @returns {Object} 对比结果
   */
  doubleBlindCompare(sql1, sql2, results1 = null, results2 = null) {
    const comparison = {
      structural: this._compareStructure(sql1, sql2),
      keyword: this._compareKeywords(sql1, sql2),
      result: null,
      consensus: 'pending',
      recommendation: '',
    };

    // 如果有执行结果，进行结果对比
    if (results1 && results2) {
      comparison.result = this._compareResults(results1, results2);
      
      if (comparison.result.isEquivalent) {
        comparison.consensus = 'agreed';
        comparison.recommendation = '两个SQL的执行结果一致，可以收集。建议选择语义等价性更强、效率更高的SQL作为标准答案。';
      } else {
        comparison.consensus = 'disagreed';
        comparison.recommendation = '两个SQL的执行结果不一致，需要专家审查。';
      }
    }

    // 结构相似度评估
    if (comparison.structural.similarity > 0.8) {
      comparison.structuralAssessment = '两个SQL结构高度相似';
    } else if (comparison.structural.similarity > 0.5) {
      comparison.structuralAssessment = '两个SQL结构部分相似，可能使用了不同的查询策略';
    } else {
      comparison.structuralAssessment = '两个SQL结构差异较大，建议重点审查';
    }

    // SQL效率对比建议
    const complexity1 = this.assessComplexity(sql1);
    const complexity2 = this.assessComplexity(sql2);
    comparison.efficiencyComparison = {
      sql1_complexity: { score: complexity1.score, level: complexity1.level },
      sql2_complexity: { score: complexity2.score, level: complexity2.level },
      moreEfficient: complexity1.score <= complexity2.score ? 'sql1' : 'sql2',
      recommendation: complexity1.score === complexity2.score
        ? '两个SQL复杂度相当'
        : `建议优先选择${complexity1.score <= complexity2.score ? 'SQL1' : 'SQL2'}（复杂度更低）`,
    };

    return comparison;
  }

  /**
   * 对比两个SQL的结构
   */
  _compareStructure(sql1, sql2) {
    const struct1 = this._extractStructureFeatures(sql1);
    const struct2 = this._extractStructureFeatures(sql2);

    const allFeatures = new Set([...Object.keys(struct1), ...Object.keys(struct2)]);
    let matchCount = 0;
    let totalCount = allFeatures.size;
    const differences = [];

    for (const feature of allFeatures) {
      const v1 = struct1[feature];
      const v2 = struct2[feature];
      if (JSON.stringify(v1) === JSON.stringify(v2)) {
        matchCount++;
      } else {
        differences.push({ feature, sql1: v1, sql2: v2 });
      }
    }

    return {
      similarity: totalCount > 0 ? matchCount / totalCount : 1,
      matchedFeatures: matchCount,
      totalFeatures: totalCount,
      differences,
    };
  }

  /**
   * 提取SQL结构特征
   */
  _extractStructureFeatures(sql) {
    const upper = sql.toUpperCase();
    return {
      hasWhere: /\bWHERE\b/.test(upper),
      hasGroupBy: /\bGROUP BY\b/.test(upper),
      hasOrderBy: /\bORDER BY\b/.test(upper),
      hasLimit: /\bLIMIT\b/.test(upper),
      hasHaving: /\bHAVING\b/.test(upper),
      hasJoin: /\bJOIN\b/.test(upper),
      hasSubquery: /\(\s*SELECT\b/i.test(sql),
      hasCTE: /\bWITH\b/.test(upper),
      hasUnion: /\bUNION\b/.test(upper),
      tableCount: (upper.match(/\bFROM\b/g) || []).length + (upper.match(/\bJOIN\b/g) || []).length,
      aggregateCount: (upper.match(/\b(AVG|COUNT|MAX|MIN|SUM)\s*\(/g) || []).length,
    };
  }

  /**
   * 对比两个SQL的关键字使用
   */
  _compareKeywords(sql1, sql2) {
    const kw1 = this.classifyKeywords(sql1);
    const kw2 = this.classifyKeywords(sql2);

    const categoryComparisons = {};
    for (const category of Object.keys(SQL_KEYWORD_CATEGORIES)) {
      const set1 = new Set(kw1[category].keywords);
      const set2 = new Set(kw2[category].keywords);
      const intersection = [...set1].filter((k) => set2.has(k));
      const union = new Set([...set1, ...set2]);

      categoryComparisons[category] = {
        name: kw1[category].name,
        sql1Only: [...set1].filter((k) => !set2.has(k)),
        sql2Only: [...set2].filter((k) => !set1.has(k)),
        shared: intersection,
        jaccard: union.size > 0 ? intersection.length / union.size : 1,
      };
    }

    return categoryComparisons;
  }

  /**
   * 对比两个SQL的执行结果
   */
  _compareResults(results1, results2) {
    if (!Array.isArray(results1) || !Array.isArray(results2)) {
      return { isEquivalent: false, reason: '结果格式无效' };
    }

    // 行数对比
    if (results1.length !== results2.length) {
      return {
        isEquivalent: false,
        reason: `行数不一致: SQL1=${results1.length}行, SQL2=${results2.length}行`,
        rowCountDiff: Math.abs(results1.length - results2.length),
      };
    }

    if (results1.length === 0) {
      return { isEquivalent: true, reason: '两个SQL都返回空结果' };
    }

    // 列对比
    const cols1 = Object.keys(results1[0]).sort();
    const cols2 = Object.keys(results2[0]).sort();

    // 值对比（忽略列名差异，按位置对比排序后的值）
    const normalize = (rows) => {
      return rows.map((row) => {
        const values = Object.values(row).map((v) => {
          if (v === null || v === undefined) return 'NULL';
          if (typeof v === 'number') return Number(v.toFixed(6));
          return String(v);
        });
        return values.sort().join('|');
      }).sort();
    };

    const norm1 = normalize(results1);
    const norm2 = normalize(results2);

    let matchCount = 0;
    for (let i = 0; i < norm1.length; i++) {
      if (norm1[i] === norm2[i]) {
        matchCount++;
      }
    }

    const matchRate = matchCount / norm1.length;
    const isEquivalent = matchRate >= 0.95;

    return {
      isEquivalent,
      matchRate: Number(matchRate.toFixed(4)),
      reason: isEquivalent
        ? `结果匹配率${(matchRate * 100).toFixed(1)}%，视为等价`
        : `结果匹配率${(matchRate * 100).toFixed(1)}%，存在差异`,
      columnComparison: {
        sql1Columns: cols1,
        sql2Columns: cols2,
        columnsMatch: JSON.stringify(cols1) === JSON.stringify(cols2),
      },
      rowCount: results1.length,
    };
  }

  // ==================== 文本-知识-SQL对齐 ====================

  /**
   * 检查文本(question) + 知识(evidence) + SQL的对齐性
   */
  checkAlignment(sql, question, evidence) {
    const checks = {
      executability: this._checkExecutability(sql),
      alignment: {},
    };

    if (question) {
      checks.alignment.questionToSQL = this._checkQuestionSQLAlignment(sql, question);
    }

    if (evidence) {
      checks.alignment.evidenceToSQL = this._checkEvidenceSQLAlignment(sql, evidence);
    }

    if (question && evidence) {
      checks.alignment.questionToEvidence = this._checkQuestionEvidenceAlignment(question, evidence);
    }

    // 综合评估
    const scores = [
      checks.alignment.questionToSQL?.score || 0,
      checks.alignment.evidenceToSQL?.score || 0,
    ].filter((s) => s > 0);

    checks.overallAlignment = scores.length > 0
      ? Number((scores.reduce((a, b) => a + b, 0) / scores.length).toFixed(2))
      : 0;

    return checks;
  }

  /**
   * SQL可执行性预检
   */
  _checkExecutability(sql) {
    const upper = sql.toUpperCase().trim();
    const issues = [];

    // 基本结构检查
    if (!upper.startsWith('SELECT') && !upper.startsWith('WITH')) {
      issues.push('SQL不是以SELECT或WITH开头');
    }
    if (!upper.includes('FROM') && !upper.startsWith('SELECT 1') && !upper.startsWith('SELECT \'')) {
      issues.push('SQL缺少FROM子句');
    }

    // 括号匹配
    const openParens = (sql.match(/\(/g) || []).length;
    const closeParens = (sql.match(/\)/g) || []).length;
    if (openParens !== closeParens) {
      issues.push(`括号不匹配: 左括号${openParens}个, 右括号${closeParens}个`);
    }

    // 引号匹配
    const singleQuotes = (sql.match(/'/g) || []).length;
    if (singleQuotes % 2 !== 0) {
      issues.push('单引号不匹配');
    }

    return {
      likely_executable: issues.length === 0,
      issues,
    };
  }

  /**
   * 检查问题和SQL的对齐
   */
  _checkQuestionSQLAlignment(sql, question) {
    const questionTokens = this._tokenize(question);
    const sqlTokens = this._tokenize(sql);

    const overlapTokens = questionTokens.filter((t) =>
      sqlTokens.some((st) => st.includes(t) || t.includes(st)),
    );

    return {
      score: questionTokens.length > 0 ? overlapTokens.length / questionTokens.length : 0,
      overlappingTerms: overlapTokens,
      questionTerms: questionTokens.length,
      sqlTerms: sqlTokens.length,
    };
  }

  /**
   * 检查evidence和SQL的对齐
   */
  _checkEvidenceSQLAlignment(sql, evidence) {
    const evidenceTokens = this._tokenize(evidence);
    const sqlLower = sql.toLowerCase();

    const matchedTerms = evidenceTokens.filter((t) => sqlLower.includes(t.toLowerCase()));

    return {
      score: evidenceTokens.length > 0 ? matchedTerms.length / evidenceTokens.length : 0,
      matchedTerms,
      totalEvidenceTerms: evidenceTokens.length,
    };
  }

  /**
   * 检查问题和evidence的对齐
   */
  _checkQuestionEvidenceAlignment(question, evidence) {
    const qTokens = this._tokenize(question);
    const eTokens = this._tokenize(evidence);
    const overlap = qTokens.filter((t) => eTokens.some((e) => e.includes(t) || t.includes(e)));

    return {
      score: qTokens.length > 0 ? overlap.length / qTokens.length : 0,
      overlappingTerms: overlap,
    };
  }

  /**
   * 简单分词（支持中英文）
   */
  _tokenize(text) {
    if (!text) return [];
    const english = text.match(/[a-zA-Z_]{2,}/g) || [];
    // 对于中文，按2-4字的滑动窗口提取
    const chinese = text.match(/[\u4e00-\u9fff]{2,4}/g) || [];
    return [...new Set([...english.map((w) => w.toLowerCase()), ...chinese])];
  }

  // ==================== 原有功能（保留并增强） ====================

  extractTables(sql) {
    const upper = sql.toUpperCase();
    const tables = [];
    
    try {
      if (upper.startsWith('WITH')) {
        const withPattern = /\bWITH\s+(\w+)\s+AS\s*\(([\s\S]*?)\)/gi;
        let withMatch;
        while ((withMatch = withPattern.exec(sql)) !== null) {
          const cteBody = withMatch[2];
          const cteUpper = cteBody.toUpperCase();
          
          const fromMatch = cteUpper.match(/\bFROM\b([\s\S]+?)(\bWHERE\b|\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|\bJOIN\b|$)/);
          if (fromMatch && fromMatch[1]) {
            const rawTables = fromMatch[1].split(',').map((t) => t.trim()).filter(Boolean);
            rawTables.forEach((t) => {
              const withoutAlias = t.replace(/\s+AS\s+.+$/i, '');
              const tableName = withoutAlias.split(/\s+/).pop();
              if (tableName && !tableName.match(/^\(|\)$/)) {
                tables.push(tableName);
              }
            });
          }
          
          const joinMatches = cteBody.matchAll(/\b(INNER\s+)?JOIN\s+(\w+)/gi);
          for (const match of joinMatches) {
            if (match[2]) tables.push(match[2]);
          }
        }
      }
      
      let mainQuery = sql;
      if (upper.startsWith('WITH')) {
        const lastSelectMatch = sql.match(/\bWITH\s+[\s\S]+?\bSELECT\s+([\s\S]+)$/i);
        if (lastSelectMatch) mainQuery = lastSelectMatch[0];
      }
      
      const mainUpper = mainQuery.toUpperCase();
      const fromMatch = mainUpper.match(/\bFROM\b([\s\S]+?)(\bWHERE\b|\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|$)/);
      if (fromMatch && fromMatch[1]) {
        const rawTables = fromMatch[1].split(',').map((t) => t.trim()).filter(Boolean);
        rawTables.forEach((t) => {
          const withoutAlias = t.replace(/\s+AS\s+.+$/i, '');
          const tableName = withoutAlias.split(/\s+/).pop();
          if (tableName && !tableName.match(/^\(|\)$/)) {
            tables.push(tableName);
          }
        });
      }
      
      const joinMatches = mainQuery.matchAll(/\b(INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN\s+(\w+)/gi);
      for (const match of joinMatches) {
        if (match[2]) tables.push(match[2]);
      }
    } catch (error) {
      logger.warn('[SQLValidationTool] 提取表名失败:', error.message);
    }
    
    return [...new Set(tables.filter((t) => t && t.length > 0 && !t.match(/^[()]+$/)))];
  }

  checkSecurity(sql) {
    const upper = sql.toUpperCase().trim();
    const errors = [];
    const warnings = [];

    const isWithClause = upper.startsWith('WITH');
    const isSelectQuery = upper.startsWith('SELECT');
    
    if (!isWithClause && !isSelectQuery) {
      errors.push({
        type: 'security',
        message: '只允许执行SELECT查询或WITH子句（CTE），请不要包含INSERT/UPDATE/DELETE/DDL等写操作。',
        severity: 'error',
      });
      return { errors, warnings };
    }

    if (isWithClause) {
      if (!upper.includes('SELECT')) {
        errors.push({
          type: 'security',
          message: 'WITH子句必须包含SELECT查询，不允许包含写操作。',
          severity: 'error',
        });
        return { errors, warnings };
      }

      const withPattern = /\bWITH\s+[\w\s,]+?\s+AS\s*\(/i;
      if (!withPattern.test(sql)) {
        warnings.push({
          type: 'syntax',
          message: 'WITH子句格式可能不规范，建议格式：WITH cte_name AS (SELECT ...)',
        });
      }

      const withMatches = sql.matchAll(/\bWITH\s+(\w+)\s+AS\s*\(([\s\S]*?)\)/gi);
      for (const match of withMatches) {
        const cteBody = match[2];
        const writeOps = [
          /\bINSERT\s+INTO\b/i, /\bUPDATE\s+\w+\s+SET\b/i, /\bDELETE\s+FROM\b/i,
          /\bDROP\s+(TABLE|DATABASE)\b/i, /\bCREATE\s+(TABLE|DATABASE)\b/i,
        ];
        for (const pattern of writeOps) {
          if (pattern.test(cteBody)) {
            errors.push({
              type: 'security',
              message: `WITH子句 "${match[1]}" 中包含写操作，不允许执行。`,
              severity: 'error',
            });
          }
        }
      }
    }

    const dangerousPatterns = [
      /\bDROP\s+(TABLE|DATABASE|INDEX|VIEW|PROCEDURE|FUNCTION|TRIGGER)\b/i,
      /\bDELETE\s+FROM\b/i, /\bUPDATE\s+\w+\s+SET\b/i, /\bINSERT\s+INTO\b/i,
      /\bALTER\s+TABLE\b/i, /\bTRUNCATE\s+TABLE\b/i,
      /\bCREATE\s+(TABLE|DATABASE|INDEX|VIEW|PROCEDURE|FUNCTION|TRIGGER)\b/i,
      /\bGRANT\b/i, /\bREVOKE\b/i, /\bEXEC\s+/i, /\bEXECUTE\s+/i,
    ];

    for (const pattern of dangerousPatterns) {
      if (pattern.test(sql)) {
        const match = sql.match(pattern);
        errors.push({
          type: 'security',
          message: `检测到危险操作 "${match[0].trim()}"，出于安全考虑拒绝执行该查询。`,
          severity: 'error',
        });
      }
    }

    if (upper.includes('SELECT *')) {
      warnings.push({
        type: 'best_practice',
        message: '建议避免使用 SELECT *，明确指定需要的字段可以提高性能。',
      });
    }

    if (/\bCROSS\s+JOIN\b/i.test(sql)) {
      warnings.push({
        type: 'performance',
        message: '检测到 CROSS JOIN，可能产生笛卡尔积。请确认这是预期的行为。',
      });
    }

    const hasMultipleCTEs = (sql.match(/\bWITH\s+\w+\s+AS\s*\(/gi) || []).length > 1;
    const hasMultipleSubqueries = (sql.match(/\([^)]*\bSELECT\b[^)]*\)/gi) || []).length > 3;
    if (hasMultipleCTEs || hasMultipleSubqueries) {
      warnings.push({
        type: 'performance',
        message: '查询包含多个CTE或子查询，可能影响性能。',
      });
    }

    return { errors, warnings };
  }

  checkSyntax(sql) {
    const errors = [];
    
    const openParens = (sql.match(/\(/g) || []).length;
    const closeParens = (sql.match(/\)/g) || []).length;
    if (openParens !== closeParens) {
      errors.push({ type: 'syntax', message: '括号不匹配', severity: 'error' });
    }

    const singleQuotes = (sql.match(/'/g) || []).length;
    const doubleQuotes = (sql.match(/"/g) || []).length;
    if (singleQuotes % 2 !== 0) {
      errors.push({ type: 'syntax', message: '单引号不匹配', severity: 'error' });
    }
    if (doubleQuotes % 2 !== 0) {
      errors.push({ type: 'syntax', message: '双引号不匹配', severity: 'error' });
    }

    return errors;
  }

  checkSchema(sql, schemaInfo) {
    const errors = [];
    const warnings = [];
    if (!schemaInfo) return { errors, warnings };

    const tables = this.extractTables(sql);
    const schemaTables = schemaInfo.tables || [];

    for (const table of tables) {
      if (schemaTables.length > 0 && !schemaTables.includes(table)) {
        errors.push({
          type: 'schema',
          message: `表 "${table}" 不存在于数据库中`,
          severity: 'error',
        });
      }
    }

    return { errors, warnings };
  }

  assessRisk(errors, warnings) {
    if (errors.some((e) => e.type === 'security')) return 'high';
    if (errors.some((e) => e.type === 'syntax' || e.type === 'schema')) return 'medium';
    if (warnings.length > 0) return 'low';
    return 'low';
  }

  extractMetadata(sql) {
    const upper = sql.toUpperCase();
    const tables = this.extractTables(sql);

    return {
      tables_used: tables,
      operations: upper.startsWith('WITH') ? ['WITH', 'SELECT'] : ['SELECT'],
      has_with_clause: upper.startsWith('WITH'),
      has_join: /\bJOIN\b/i.test(sql),
      has_subquery: /\([^)]*\bSELECT\b[^)]*\)/i.test(sql),
      has_group_by: /\bGROUP BY\b/i.test(sql),
      has_order_by: /\bORDER BY\b/i.test(sql),
      has_limit: /\bLIMIT\b/i.test(sql),
    };
  }

  async _call(input) {
    const { sql, check_schema = false, schema_info, compare_sql, compare_results, evidence, question } = input;
    const trimmedSql = sql.trim();

    try {
      logger.info('[SQLValidationTool] 开始SQL校验（增强版）:', {
        sql: trimmedSql.substring(0, 50),
        hasCompareSQL: !!compare_sql,
        hasEvidence: !!evidence,
      });

      // 安全性检查
      const securityCheck = this.checkSecurity(trimmedSql);
      const errors = [...securityCheck.errors];
      const warnings = [...securityCheck.warnings];

      // 语法检查
      errors.push(...this.checkSyntax(trimmedSql));

      // Schema检查
      if (check_schema && schema_info) {
        const schemaCheck = this.checkSchema(trimmedSql, schema_info);
        errors.push(...schemaCheck.errors);
        warnings.push(...schemaCheck.warnings);
      }

      const riskLevel = this.assessRisk(errors, warnings);
      const metadata = this.extractMetadata(trimmedSql);

      // 7类关键字分类
      const complexity = this.assessComplexity(trimmedSql);

      const result = {
        valid: errors.length === 0,
        errors: errors.length > 0 ? errors : undefined,
        warnings: warnings.length > 0 ? warnings : undefined,
        risk_level: riskLevel,
        metadata,
        complexity: {
          score: complexity.score,
          level: complexity.level,
          factors: complexity.factors,
        },
        keyword_classification: complexity.keywordCategories,
      };

      // 双盲SQL对比
      if (compare_sql) {
        result.double_blind_comparison = this.doubleBlindCompare(
          trimmedSql, compare_sql.trim(),
          compare_results?.sql1_results,
          compare_results?.sql2_results,
        );
      }

      // 文本-知识-SQL对齐
      if (question || evidence) {
        result.alignment = this.checkAlignment(trimmedSql, question, evidence);
      }

      logger.info('[SQLValidationTool] 校验完成:', {
        valid: result.valid,
        riskLevel,
        complexityLevel: complexity.level,
        hasComparison: !!result.double_blind_comparison,
      });

      return JSON.stringify(result, null, 2);
    } catch (error) {
      logger.error('[SQLValidationTool] 校验失败:', error);
      return JSON.stringify({
        valid: false,
        errors: [{ type: 'syntax', message: `校验过程出错: ${error.message}`, severity: 'error' }],
        risk_level: 'high',
        metadata: {},
      }, null, 2);
    }
  }
}

module.exports = SQLValidationTool;
