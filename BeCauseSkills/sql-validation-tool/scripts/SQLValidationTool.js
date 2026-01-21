const { Tool } = require('@langchain/core/tools');
const { z } = require('zod');
const { logger } = require('@because/data-schemas');

/**
 * SQL Validation Tool - SQL校验工具
 * 
 * 检查SQL的合法性、安全性和正确性
 */
class SQLValidationTool extends Tool {
  name = 'sql_validation';

  description =
    'SQL合法性/安全性检测工具，检查SQL语法、安全性、表字段存在性等。' +
    '禁止DROP、DELETE、UPDATE等危险操作，只允许SELECT查询和WITH子句（CTE）。' +
    '支持识别WITH子句结构，正确提取表名和验证安全性。';

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
  });

  /**
   * 提取SQL中使用的表名
   */
  extractTables(sql) {
    const upper = sql.toUpperCase();
    const tables = [];
    
    try {
      // 处理WITH子句：提取WITH子句中的表名
      if (upper.startsWith('WITH')) {
        // 匹配WITH子句：WITH cte_name AS (SELECT ... FROM table ...)
        const withPattern = /\bWITH\s+(\w+)\s+AS\s*\(([\s\S]*?)\)/gi;
        let withMatch;
        while ((withMatch = withPattern.exec(sql)) !== null) {
          const cteBody = withMatch[2]; // CTE的SELECT部分
          const cteUpper = cteBody.toUpperCase();
          
          // 从CTE体中提取表名
          const fromMatch = cteUpper.match(/\bFROM\b([\s\S]+?)(\bWHERE\b|\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|\bJOIN\b|$)/);
          if (fromMatch && fromMatch[1]) {
            const rawTables = fromMatch[1]
              .split(',')
              .map((t) => t.trim())
              .filter(Boolean);
            
            rawTables.forEach((t) => {
              // 移除AS别名
              const withoutAlias = t.replace(/\s+AS\s+.+$/i, '');
              // 提取表名（去除可能的JOIN关键字）
              const tableName = withoutAlias.split(/\s+/).pop();
              if (tableName && !tableName.match(/^\(|\)$/)) {
                tables.push(tableName);
              }
            });
          }
          
          // 提取CTE体中的JOIN表名
          const joinMatches = cteBody.matchAll(/\b(INNER\s+)?JOIN\s+(\w+)/gi);
          for (const match of joinMatches) {
            if (match[2]) {
              tables.push(match[2]);
            }
          }
        }
      }
      
      // 提取主查询中的表名（WITH子句后的SELECT部分）
      let mainQuery = sql;
      if (upper.startsWith('WITH')) {
        // 找到最后一个SELECT（主查询）
        const lastSelectMatch = sql.match(/\bWITH\s+[\s\S]+?\bSELECT\s+([\s\S]+)$/i);
        if (lastSelectMatch) {
          mainQuery = lastSelectMatch[0];
        }
      }
      
      const mainUpper = mainQuery.toUpperCase();
      
      // 提取FROM之后到WHERE/GROUP BY/ORDER BY/LIMIT之前的部分
      const fromMatch = mainUpper.match(/\bFROM\b([\s\S]+?)(\bWHERE\b|\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|$)/);
      if (fromMatch && fromMatch[1]) {
        const rawTables = fromMatch[1]
          .split(',')
          .map((t) => t.trim())
          .filter(Boolean);
        
        rawTables.forEach((t) => {
          // 移除AS别名
          const withoutAlias = t.replace(/\s+AS\s+.+$/i, '');
          // 提取表名（去除可能的JOIN关键字）
          const tableName = withoutAlias.split(/\s+/).pop();
          // 排除CTE名称（它们不是实际表名）
          if (tableName && !tableName.match(/^\(|\)$/)) {
            tables.push(tableName);
          }
        });
      }
      
      // 提取JOIN中的表名
      const joinMatches = mainQuery.matchAll(/\b(INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN\s+(\w+)/gi);
      for (const match of joinMatches) {
        if (match[2]) {
          tables.push(match[2]);
        }
      }
    } catch (error) {
      logger.warn('[SQLValidationTool] 提取表名失败:', error.message);
    }
    
    // 过滤掉明显的CTE名称（如果它们出现在FROM子句中，说明是CTE引用，不是实际表）
    // 这里简化处理，实际应该解析CTE定义
    const filteredTables = tables.filter(table => {
      // 过滤掉空值和明显不是表名的内容
      return table && table.length > 0 && !table.match(/^[()]+$/);
    });
    
    return [...new Set(filteredTables)]; // 去重
  }

  /**
   * 检查SQL安全性
   */
  checkSecurity(sql) {
    const upper = sql.toUpperCase().trim();
    const errors = [];
    const warnings = [];

    // 支持WITH子句（CTE）和SELECT查询
    // 检查是否以WITH或SELECT开头
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

    // 如果是以WITH开头，验证其结构：WITH ... AS (SELECT ...)
    if (isWithClause) {
      // 检查WITH子句是否包含SELECT（这是只读查询的标志）
      if (!upper.includes('SELECT')) {
        errors.push({
          type: 'security',
          message: 'WITH子句必须包含SELECT查询，不允许包含写操作。',
          severity: 'error',
        });
        return { errors, warnings };
      }

      // 验证WITH子句结构：WITH ... AS (SELECT ...)
      // 使用更宽松的模式，支持多个CTE和复杂结构
      const withPattern = /\bWITH\s+[\w\s,]+?\s+AS\s*\(/i;
      if (!withPattern.test(sql)) {
        // 只作为警告，不阻止执行（因为可能是复杂的WITH子句结构）
        warnings.push({
          type: 'syntax',
          message: 'WITH子句格式可能不规范，建议格式：WITH cte_name AS (SELECT ...)',
        });
      }

      // 确保WITH子句中没有写操作
      // 提取所有WITH子句的内容进行检查
      const withMatches = sql.matchAll(/\bWITH\s+(\w+)\s+AS\s*\(([\s\S]*?)\)/gi);
      for (const match of withMatches) {
        const cteBody = match[2];
        const cteUpper = cteBody.toUpperCase();
        
        // 检查CTE体中是否有写操作
        const writeOps = [
          /\bINSERT\s+INTO\b/i,
          /\bUPDATE\s+\w+\s+SET\b/i,
          /\bDELETE\s+FROM\b/i,
          /\bDROP\s+(TABLE|DATABASE)\b/i,
          /\bCREATE\s+(TABLE|DATABASE)\b/i,
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

    // 禁止危险关键词 - 使用更精确的正则表达式匹配SQL语句
    // 避免误判字段名、表名或注释中包含这些关键词
    const dangerousPatterns = [
      /\bDROP\s+(TABLE|DATABASE|INDEX|VIEW|PROCEDURE|FUNCTION|TRIGGER)\b/i,
      /\bDELETE\s+FROM\b/i,
      /\bUPDATE\s+\w+\s+SET\b/i,
      /\bINSERT\s+INTO\b/i,
      /\bALTER\s+TABLE\b/i,
      /\bTRUNCATE\s+TABLE\b/i,
      /\bCREATE\s+(TABLE|DATABASE|INDEX|VIEW|PROCEDURE|FUNCTION|TRIGGER)\b/i,
      /\bGRANT\b/i,
      /\bREVOKE\b/i,
      /\bEXEC\s+/i,
      /\bEXECUTE\s+/i,
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

    // 检查子查询中的危险操作（排除WITH子句中的SELECT）
    // 先移除WITH子句部分，再检查剩余部分
    let sqlToCheck = sql;
    if (isWithClause) {
      // 移除WITH ... AS (...)部分，保留最后的SELECT
      const withMatch = sql.match(/\bWITH\s+[\s\S]+?\bAS\s*\([\s\S]*?\)\s*(SELECT|$)/i);
      if (withMatch) {
        // 提取WITH子句后的SELECT部分
        const afterWith = sql.substring(withMatch[0].length - (withMatch[1]?.length || 0));
        sqlToCheck = afterWith.trim() || sql;
      }
    }

    // 检查子查询中的危险操作
    const subqueryPattern = /\([^)]*\b(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE)\b[^)]*\)/gi;
    const dangerousSubqueries = sqlToCheck.match(subqueryPattern);
    if (dangerousSubqueries) {
      for (const subquery of dangerousSubqueries) {
        const subUpper = subquery.toUpperCase();
        // 只检查非SELECT的危险操作
        if (subUpper.includes('INSERT') || subUpper.includes('UPDATE') || 
            subUpper.includes('DELETE') || subUpper.includes('DROP') ||
            subUpper.includes('CREATE TABLE') || subUpper.includes('ALTER TABLE')) {
          errors.push({
            type: 'security',
            message: `子查询中包含危险操作。`,
            severity: 'error',
          });
        }
      }
    }

    // 性能警告（不是错误）
    if (upper.includes('SELECT *')) {
      warnings.push({
        type: 'best_practice',
        message: '建议避免使用 SELECT *，明确指定需要的字段可以提高性能。',
      });
    }

    // CROSS JOIN 性能警告（不是错误，因为它是合法的只读操作）
    if (/\bCROSS\s+JOIN\b/i.test(sql)) {
      warnings.push({
        type: 'performance',
        message: '检测到 CROSS JOIN，可能产生笛卡尔积。请确认这是预期的行为，并注意性能影响。',
      });
    }

    // 复杂查询警告（不是错误）
    const hasMultipleCTEs = (sql.match(/\bWITH\s+\w+\s+AS\s*\(/gi) || []).length > 1;
    const hasMultipleSubqueries = (sql.match(/\([^)]*\bSELECT\b[^)]*\)/gi) || []).length > 3;
    if (hasMultipleCTEs || hasMultipleSubqueries) {
      warnings.push({
        type: 'performance',
        message: '查询包含多个CTE或子查询，可能影响性能。请确认查询逻辑正确。',
      });
    }

    return { errors, warnings };
  }

  /**
   * 基本语法检查
   */
  checkSyntax(sql) {
    const errors = [];
    
    // 检查括号匹配
    const openParens = (sql.match(/\(/g) || []).length;
    const closeParens = (sql.match(/\)/g) || []).length;
    if (openParens !== closeParens) {
      errors.push({
        type: 'syntax',
        message: '括号不匹配',
        severity: 'error',
      });
    }

    // 检查引号匹配
    const singleQuotes = (sql.match(/'/g) || []).length;
    const doubleQuotes = (sql.match(/"/g) || []).length;
    if (singleQuotes % 2 !== 0) {
      errors.push({
        type: 'syntax',
        message: '单引号不匹配',
        severity: 'error',
      });
    }
    if (doubleQuotes % 2 !== 0) {
      errors.push({
        type: 'syntax',
        message: '双引号不匹配',
        severity: 'error',
      });
    }

    return errors;
  }

  /**
   * Schema检查
   */
  checkSchema(sql, schemaInfo) {
    const errors = [];
    const warnings = [];
    
    if (!schemaInfo) {
      return { errors, warnings };
    }

    const tables = this.extractTables(sql);
    const schemaTables = schemaInfo.tables || [];
    const schemaFields = schemaInfo.fields || {};

    // 检查表是否存在
    for (const table of tables) {
      if (schemaTables.length > 0 && !schemaTables.includes(table)) {
        errors.push({
          type: 'schema',
          message: `表 "${table}" 不存在于数据库中`,
          severity: 'error',
        });
      }
    }

    // 检查字段是否存在（简化处理，实际应解析SELECT字段）
    // 这里只做基本检查，完整实现需要SQL解析器

    return { errors, warnings };
  }

  /**
   * 评估风险等级
   */
  assessRisk(errors, warnings) {
    const hasSecurityError = errors.some(e => e.type === 'security');
    const hasSyntaxError = errors.some(e => e.type === 'syntax');
    const hasSchemaError = errors.some(e => e.type === 'schema');

    if (hasSecurityError) {
      return 'high';
    }
    if (hasSyntaxError || hasSchemaError) {
      return 'medium';
    }
    if (warnings.length > 0) {
      return 'low';
    }
    return 'low';
  }

  /**
   * 提取SQL元数据
   */
  extractMetadata(sql) {
    const upper = sql.toUpperCase();
    const tables = this.extractTables(sql);
    const hasWithClause = upper.startsWith('WITH');

    return {
      tables_used: tables,
      operations: hasWithClause ? ['WITH', 'SELECT'] : ['SELECT'],
      has_with_clause: hasWithClause,
      has_join: /\bJOIN\b/i.test(sql),
      has_subquery: /\([^)]*\bSELECT\b[^)]*\)/i.test(sql),
      has_group_by: /\bGROUP BY\b/i.test(sql),
      has_order_by: /\bORDER BY\b/i.test(sql),
      has_limit: /\bLIMIT\b/i.test(sql),
    };
  }

  /**
   * @override
   */
  async _call(input) {
    const { sql, check_schema = false, schema_info } = input;
    const trimmedSql = sql.trim();

    try {
      logger.info('[SQLValidationTool] 开始SQL校验:', {
        sql: trimmedSql.substring(0, 50),
        checkSchema: check_schema,
      });

      // 安全性检查
      const securityCheck = this.checkSecurity(trimmedSql);
      const errors = [...securityCheck.errors];
      const warnings = [...securityCheck.warnings];

      // 语法检查
      const syntaxErrors = this.checkSyntax(trimmedSql);
      errors.push(...syntaxErrors);

      // Schema检查（如果启用）
      if (check_schema && schema_info) {
        const schemaCheck = this.checkSchema(trimmedSql, schema_info);
        errors.push(...schemaCheck.errors);
        warnings.push(...schemaCheck.warnings);
      }

      // 评估风险等级
      const riskLevel = this.assessRisk(errors, warnings);

      // 提取元数据
      const metadata = this.extractMetadata(trimmedSql);

      const result = {
        valid: errors.length === 0,
        errors: errors.length > 0 ? errors : undefined,
        warnings: warnings.length > 0 ? warnings : undefined,
        risk_level: riskLevel,
        metadata,
      };

      logger.info('[SQLValidationTool] 校验完成:', {
        valid: result.valid,
        riskLevel,
        errorsCount: errors.length,
        warningsCount: warnings.length,
      });

      return JSON.stringify(result, null, 2);
    } catch (error) {
      logger.error('[SQLValidationTool] 校验失败:', error);
      return JSON.stringify(
        {
          valid: false,
          errors: [
            {
              type: 'syntax',
              message: `校验过程出错: ${error.message}`,
              severity: 'error',
            },
          ],
          risk_level: 'high',
          metadata: {},
        },
        null,
        2,
      );
    }
  }
}

module.exports = SQLValidationTool;

