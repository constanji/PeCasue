# Financial 数据库列名含义说明

本文档详细说明 financial 数据库中所有表的列名含义，特别关注 district 表中的 A2-A16 字段。

---

## 一、district 表（地区表）

### 基础字段

1. **district_id** (integer)
   - **含义**: 地区ID，分支机构位置
   - **用途**: 主键，唯一标识一个地区/分支机构

### A 系列字段详解（A2-A16）

2. **A2** (text)
   - **含义**: 地区名称 (district_name)
   - **用途**: 存储地区的具体名称（如 'Sokolov'）
   - **示例查询**: `WHERE A2 = 'Sokolov'`

3. **A3** (text)
   - **含义**: 区域 (region)
   - **用途**: 存储地区所属的大区域名称
   - **示例值**: 'east Bohemia', 'north Bohemia', 'south Bohemia'
   - **示例查询**: `WHERE A3 = 'east Bohemia'`

4. **A4** (text)
   - **含义**: 居民数量 (number of inhabitants)
   - **用途**: 存储该地区的居民总数
   - **用途说明**: 可用于排序找出"居民数量最多的分支机构"

5. **A5** (text)
   - **含义**: 居民数 < 499 的市镇数量
   - **说明**: 该地区内居民数小于499的市镇数量
   - **层级关系**: municipality < district < region

6. **A6** (text)
   - **含义**: 居民数 500-1999 的市镇数量
   - **说明**: 该地区内居民数在500-1999之间的市镇数量
   - **层级关系**: municipality < district < region

7. **A7** (text)
   - **含义**: 居民数 2000-9999 的市镇数量
   - **说明**: 该地区内居民数在2000-9999之间的市镇数量
   - **层级关系**: municipality < district < region

8. **A8** (integer)
   - **含义**: 居民数 > 10000 的市镇数量
   - **说明**: 该地区内居民数大于10000的市镇数量
   - **层级关系**: municipality < district < region

9. **A9** (integer)
   - **含义**: [无用字段]
   - **说明**: 标记为 "not useful"，查询时通常不使用

10. **A10** (real)
    - **含义**: 城市居民比例 (ratio of urban inhabitants)
    - **用途**: 该地区城市居民占总居民的比例
    - **数据类型**: 实数（比例值）

11. **A11** (integer)
    - **含义**: 平均工资 (average salary)
    - **用途**: 该地区的平均薪资水平
    - **业务说明**: 薪资与收入含义相近，可互换使用
    - **示例查询**: `WHERE A11 > 8000`, `WHERE A11 BETWEEN 6000 AND 10000`
    - **常用场景**: 查询高薪地区、薪资区间筛选

12. **A12** (real)
    - **含义**: 1995年失业率 (unemployment rate 1995)
    - **用途**: 记录1995年该地区的失业率
    - **常用场景**: 与A13结合计算失业率增幅

13. **A13** (real)
    - **含义**: 1996年失业率 (unemployment rate 1996)
    - **用途**: 记录1996年该地区的失业率
    - **常用场景**: 与A12结合计算失业率增幅
    - **计算公式**: 失业率增幅 = [(A13 - A12) / A12] * 100

14. **A14** (integer)
    - **含义**: 每1000居民中的企业家数量 (no. of entrepreneurs per 1000 inhabitants)
    - **用途**: 反映该地区的创业活跃度
    - **说明**: 按每1000居民计算的企业家数量

15. **A15** (integer)
    - **含义**: 1995年犯罪数量 (no. of committed crimes 1995)
    - **用途**: 记录1995年该地区发生的犯罪案件数
    - **示例查询**: 查询犯罪数量第二高的地区

16. **A16** (integer)
    - **含义**: 1996年犯罪数量 (no. of committed crimes 1996)
    - **用途**: 记录1996年该地区发生的犯罪案件数
    - **说明**: 可用于分析犯罪趋势

---

## 二、client 表（客户表）

### 字段说明

1. **client_id** (integer)
   - **含义**: 客户ID，唯一标识
   - **用途**: 主键

2. **gender** (text)
   - **含义**: 性别
   - **取值**: 
     - `'F'` = 女性 (Female)
     - `'M'` = 男性 (Male)
   - **示例查询**: `WHERE gender = 'F'`

3. **birth_date** (date)
   - **含义**: 出生日期
   - **用途**: 用于计算年龄、筛选年龄段
   - **示例查询**: `WHERE STRFTIME('%Y', birth_date) < '1950'`
   - **年龄判断规则**:
     - 若 A 出生日期 < B 出生日期 → B 更年轻
     - 若 A 出生日期 > B 出生日期 → B 比 A 年长

4. **district_id** (integer)
   - **含义**: 地区ID（外键）
   - **关联**: 关联到 district 表的 district_id
   - **用途**: 标识客户所属的地区

---

## 三、account 表（账户表）

### 字段说明

1. **account_id** (integer)
   - **含义**: 账户ID，唯一标识
   - **用途**: 主键

2. **district_id** (integer)
   - **含义**: 地区ID（外键）
   - **关联**: 关联到 district 表的 district_id
   - **用途**: 标识账户开户的地区/分支机构

3. **frequency** (text)
   - **含义**: 对账单频率
   - **取值**:
     - `"POPLATEK MESICNE"` = 月度对账（每月发放）
     - `"POPLATEK TYDNE"` = 周度对账（每周发放）
     - `"POPLATEK PO OBRATU"` = 交易后对账（每笔交易后发放）
   - **示例查询**: `WHERE frequency = 'POPLATEK MESICNE'`

4. **date** (date)
   - **含义**: 账户创建日期
   - **格式**: YYMMDD（如 930101 表示 1993年1月1日）
   - **示例查询**: `WHERE STRFTIME('%Y', date) = '1993'`

---

## 四、disp 表（账户分配表）

### 字段说明

1. **disp_id** (integer)
   - **含义**: 分配ID，唯一标识
   - **用途**: 主键

2. **client_id** (integer)
   - **含义**: 客户ID（外键）
   - **关联**: 关联到 client 表的 client_id

3. **account_id** (integer)
   - **含义**: 账户ID（外键）
   - **关联**: 关联到 account 表的 account_id

4. **type** (text)
   - **含义**: 分配类型
   - **取值**:
     - `"OWNER"` = 账户所有者（有权签发永久订单或申请贷款）
     - `"USER"` = 用户
     - `"DISPONENT"` = 授权人
   - **业务规则**: 
     - 只有 `type = 'OWNER'` 的客户才具备贷款资格
     - 查询贷款相关时通常需要 `WHERE type = 'OWNER'`

---

## 五、card 表（信用卡表）

### 字段说明

1. **card_id** (integer)
   - **含义**: 信用卡ID，唯一标识
   - **用途**: 主键

2. **disp_id** (integer)
   - **含义**: 分配ID（外键）
   - **关联**: 关联到 disp 表的 disp_id

3. **type** (text)
   - **含义**: 信用卡类型
   - **取值**:
     - `"junior"` = 初级信用卡
     - `"classic"` = 标准信用卡
     - `"gold"` = 高级信用卡（高级别）
   - **示例查询**: `WHERE type = 'gold'`

4. **issued** (date)
   - **含义**: 发卡日期
   - **格式**: YYMMDD

---

## 六、loan 表（贷款表）

### 字段说明

1. **loan_id** (integer)
   - **含义**: 贷款ID，唯一标识
   - **用途**: 主键

2. **account_id** (integer)
   - **含义**: 账户ID（外键）
   - **关联**: 关联到 account 表的 account_id

3. **date** (date)
   - **含义**: 贷款批准日期
   - **示例查询**: `WHERE STRFTIME('%Y', date) = '1997'`

4. **amount** (integer)
   - **含义**: 批准金额
   - **单位**: 美元 (USD)
   - **示例查询**: `WHERE amount < 100000`, `WHERE amount >= 250000`

5. **duration** (integer)
   - **含义**: 贷款期限
   - **单位**: 月
   - **示例查询**: `WHERE duration > 12`（贷款期限超过12个月）

6. **payments** (real)
   - **含义**: 月还款额
   - **单位**: 美元/月

7. **status** (text)
   - **含义**: 还款状态
   - **取值**:
     - `'A'` = 合同完成，无问题（无问题还清）
     - `'B'` = 合同完成，贷款未还清
     - `'C'` = 运行中合同，目前正常（运行正常）
     - `'D'` = 运行中合同，客户欠款（客户负债）
   - **常用查询**:
     - 运行中的合同: `WHERE status IN ('C', 'D')`
     - 无问题还清: `WHERE status = 'A'`
     - 客户负债: `WHERE status = 'D'`

---

## 七、order 表（订单/转账表）

### 字段说明

1. **order_id** (integer)
   - **含义**: 订单ID，唯一标识
   - **用途**: 主键

2. **account_id** (integer)
   - **含义**: 账户ID（外键）
   - **关联**: 关联到 account 表的 account_id

3. **bank_to** (text)
   - **含义**: 收款银行
   - **格式**: 两字母代码

4. **account_to** (integer)
   - **含义**: 收款账户

5. **amount** (real)
   - **含义**: 转账金额
   - **单位**: 美元

6. **k_symbol** (text)
   - **含义**: 支付目的/特征 (purpose of the payment)
   - **取值**:
     - `"POJISTNE"` = 保险支付 (insurance payment)
     - `"SIPO"` = 家庭支付 (household payment)
     - `"LEASING"` = 租赁 (leasing)
     - `"UVER"` = 贷款支付 (loan payment)

---

## 八、trans 表（交易表）

### 字段说明

1. **trans_id** (integer)
   - **含义**: 交易ID，唯一标识
   - **用途**: 主键

2. **account_id** (integer)
   - **含义**: 账户ID（外键）
   - **关联**: 关联到 account 表的 account_id

3. **date** (date)
   - **含义**: 交易日期
   - **示例查询**: `WHERE STRFTIME('%Y', date) = '1998'`, `WHERE date LIKE '1996-01%'`

4. **type** (text)
   - **含义**: 交易类型（+/-）
   - **取值**:
     - `"PRIJEM"` = 存款（credit）
     - `"VYDAJ"` = 取款（withdrawal，非信用卡取款）
   - **示例查询**: `WHERE type = 'VYDAJ'`（非信用卡取款）

5. **operation** (text)
   - **含义**: 交易方式
   - **取值**:
     - `"VYBER KARTOU"` = 信用卡取款
     - `"VKLAD"` = 现金存款
     - `"PREVOD Z UCTU"` = 从其他银行收款
     - `"VYBER"` = 现金取款
     - `"PREVOD NA UCET"` = 转账到其他银行
   - **示例查询**: 
     - `WHERE operation = 'VYBER KARTOU'`（信用卡取款）
     - `WHERE operation = 'VYBER'`（现金取款）

6. **amount** (integer)
   - **含义**: 交易金额
   - **单位**: 美元 (USD)
   - **说明**: 可以为正数或负数

7. **balance** (integer)
   - **含义**: 交易后余额
   - **单位**: 美元 (USD)
   - **用途**: 用于计算增长率
   - **计算公式**: 增长率 = [(日期A余额 - 日期B余额) / 日期B余额] * 100%

8. **k_symbol** (text)
   - **含义**: 交易特征
   - **取值**:
     - `"POJISTNE"` = 保险支付
     - `"SLUZBY"` = 对账单费用（payment for statement）
     - `"UROK"` = 利息收入（interest credited）
     - `"SANKC. UROK"` = 负余额的罚息（sanction interest if negative balance）
     - `"SIPO"` = 家庭支付（household）
     - `"DUCHOD"` = 养老金（old-age pension）
     - `"UVER"` = 贷款支付（loan payment）

9. **bank** (text)
   - **含义**: 对方银行
   - **格式**: 两字母代码
   - **说明**: 每个银行有唯一的两字母代码

10. **account** (integer)
    - **含义**: 对方账户

---

## 九、A系列字段汇总表

| 字段 | 数据类型 | 含义 | 常用场景 |
|------|---------|------|---------|
| A2 | text | 地区名称 | 地区筛选、显示地区名 |
| A3 | text | 区域 | 区域筛选（如波希米亚地区） |
| A4 | text | 居民数量 | 排序找"最大的分支机构" |
| A5 | text | 小市镇数量（<499人） | 地区结构分析 |
| A6 | text | 中小市镇数量（500-1999人） | 地区结构分析 |
| A7 | text | 中等市镇数量（2000-9999人） | 地区结构分析 |
| A8 | integer | 大市镇数量（>10000人） | 地区结构分析 |
| A9 | integer | [无用字段] | 通常不使用 |
| A10 | real | 城市居民比例 | 城市化程度分析 |
| A11 | integer | 平均工资 | **最常用** - 薪资筛选、收入分析 |
| A12 | real | 1995年失业率 | 与A13一起计算失业率增幅 |
| A13 | real | 1996年失业率 | 与A12一起计算失业率增幅 |
| A14 | integer | 每1000居民企业家数 | 创业活跃度分析 |
| A15 | integer | 1995年犯罪数量 | **常用** - 犯罪相关查询 |
| A16 | integer | 1996年犯罪数量 | 犯罪趋势分析 |

---

## 十、常用字段组合查询

### 1. 地区相关查询
```sql
-- 使用 A2（地区名称）和 A3（区域）
SELECT * FROM district WHERE A2 = 'Sokolov' AND A3 = 'north Bohemia'
```

### 2. 薪资相关查询
```sql
-- 使用 A11（平均工资）
SELECT * FROM district WHERE A11 > 10000  -- 高薪地区
SELECT * FROM district WHERE A11 BETWEEN 6000 AND 10000  -- 中等薪资地区
```

### 3. 失业率相关查询
```sql
-- 使用 A12 和 A13 计算失业率增幅
SELECT district_id, (A13 - A12) / A12 * 100 AS unemployment_increase
FROM district
```

### 4. 犯罪相关查询
```sql
-- 使用 A15（1995年犯罪数量）
SELECT * FROM district ORDER BY A15 DESC LIMIT 1  -- 犯罪最多的地区
SELECT * FROM district ORDER BY A15 DESC LIMIT 1, 1  -- 犯罪第二多的地区
```

### 5. 居民数量查询
```sql
-- 使用 A4（居民数量）排序
SELECT * FROM district ORDER BY A4 DESC LIMIT 1  -- 居民最多的分支机构
```

---

## 十一、字段使用频率统计

根据 QA 数据分析，最常用的 A 系列字段：

1. **A11** (平均工资) - 出现频率最高
2. **A3** (区域) - 区域筛选常用
3. **A2** (地区名称) - 地区查询常用
4. **A15** (1995年犯罪数量) - 犯罪相关查询
5. **A12/A13** (失业率) - 失业率分析
6. **A4** (居民数量) - 排序查询

---

## 使用说明

1. **A 系列字段**：主要在 `district` 表中，用于存储地区的经济和社会统计数据
2. **字段映射**：查询时需要理解自然语言到字段的映射（如"平均薪资" → A11）
3. **计算公式**：部分字段需要结合使用计算指标（如失业率增幅、增长率）
4. **业务规则**：某些字段有特定的业务含义（如 disp.type = 'OWNER' 才能贷款）

建议在构建 RAG 系统或语义模型时，将这些字段含义作为知识库，帮助模型理解数据库结构和业务语义。

---

## 十二、Join Graph（表关联路径图）

```
district ─── client        （client.district_id = district.district_id）
district ─── account       （account.district_id = district.district_id）
client ───── disp ───── account  （disp 是 client↔account 的桥表）
account ──── loan          （loan.account_id = account.account_id）
account ──── trans         （trans.account_id = account.account_id）
account ──── order         （order.account_id = account.account_id）
disp ─────── card          （card.disp_id = disp.disp_id）
```

### Join Rules（最短路径规则）

| 需要的数据 | 最短 JOIN 路径 | 说明 |
|-----------|---------------|------|
| client + district 属性 | `client JOIN district ON client.district_id = district.district_id` | **不要** 经过 disp 和 account |
| account + district 属性 | `account JOIN district ON account.district_id = district.district_id` | 直接关联 |
| client + account 属性 | `client JOIN disp ON ... JOIN account ON ...` | **必须经过 disp 桥表** |
| account + loan 属性 | `account JOIN loan ON account.account_id = loan.account_id` | 直接关联 |
| account + trans 属性 | `account JOIN trans ON account.account_id = trans.account_id` | 直接关联 |
| account + order 属性 | `account JOIN [order] ON account.account_id = order.account_id` | 直接关联 |
| client + card 属性 | `client JOIN disp ON ... JOIN card ON disp.disp_id = card.disp_id` | 经过 disp |

### ⚠️ 常见错误：过度 JOIN

- 如果问题只涉及 **client 和 district** 的属性（如性别、出生日期、地区名称），
  **不要** JOIN account 或 disp 表。最短路径是 `client → district`。
- 如果问题只涉及 **account 和 district** 的属性，
  **不要** JOIN client 或 disp 表。最短路径是 `account → district`。
- 只有当问题同时涉及 **client 和 account** 的属性时，才需要经过 disp。

---

## 十三、枚举值语义映射（自然语言 → 数据库实际值）

数据库中的枚举值使用捷克语，查询时必须使用数据库中的实际值，不能使用英文翻译。

### trans.operation（交易方式）
| 自然语言 | 数据库实际值 |
|---------|-----------|
| credit card withdrawal（信用卡取款） | `'VYBER KARTOU'` |
| cash withdrawal（现金取款） | `'VYBER'` |
| cash deposit / credit in cash（现金存款） | `'VKLAD'` |
| collection from another bank（从其他银行收款） | `'PREVOD Z UCTU'` |
| remittance to another bank（转账到其他银行） | `'PREVOD NA UCET'` |

### trans.type（交易类型）
| 自然语言 | 数据库实际值 |
|---------|-----------|
| credit / deposit（存入） | `'PRIJEM'` |
| withdrawal（支出） | `'VYDAJ'` |

### trans.k_symbol / order.k_symbol（交易/支付目的）
| 自然语言 | 数据库实际值 |
|---------|-----------|
| insurance payment（保险支付） | `'POJISTNE'` |
| household payment（家庭支付） | `'SIPO'` |
| leasing（租赁） | `'LEASING'` |
| loan payment（贷款支付） | `'UVER'` |
| interest credited（利息收入） | `'UROK'` |
| sanction interest / negative balance penalty（罚息） | `'SANKC. UROK'` |
| payment for statement（对账单费用） | `'SLUZBY'` |
| old-age pension（养老金） | `'DUCHOD'` |

### account.frequency（对账单频率）
| 自然语言 | 数据库实际值 |
|---------|-----------|
| monthly issuance（月度发放） | `'POPLATEK MESICNE'` |
| weekly issuance（周度发放） | `'POPLATEK TYDNE'` |
| issuance after transaction（交易后发放） | `'POPLATEK PO OBRATU'` |

### loan.status（贷款状态）
| 自然语言 | 数据库实际值 |
|---------|-----------|
| finished, no problems（已还清） | `'A'` |
| finished, unpaid（已结束未还清） | `'B'` |
| running, OK（进行中正常） | `'C'` |
| running, in debt（进行中违约） | `'D'` |

### disp.type（分配类型）
| 自然语言 | 数据库实际值 |
|---------|-----------|
| owner（所有者） | `'OWNER'` |
| user（使用者） | `'USER'` |
| authorized person / disponent（授权人） | `'DISPONENT'` |

### card.type（信用卡类型）
| 自然语言 | 数据库实际值 |
|---------|-----------|
| junior card | `'junior'` |
| classic card | `'classic'` |
| gold card | `'gold'` |

### client.gender（性别）
| 自然语言 | 数据库实际值 |
|---------|-----------|
| female（女性） | `'F'` |
| male（男性） | `'M'` |

---

## 十四、SQL 生成规则

### 规则 1：只返回问题明确要求的字段
- 不要返回额外的列。如果问题只问 "account_id"，不要附带返回 amount、frequency 等。
- 如果问题问 "how many"，返回 COUNT(...)，不要返回明细行。

### 规则 2：使用最短 JOIN 路径
- 参考上方的 Join Rules 表，选择涉及最少表的路径。
- 如果问题只涉及 client 和 district，**不要** JOIN disp 和 account。

### 规则 3：优先使用简洁写法
- 找最大/最小值：优先 `ORDER BY ... LIMIT 1` 而非子查询或 CTE。
- 年份对比：优先 `SUM(CASE WHEN ... THEN ... END)` 而非多个 CTE。
- 避免不必要的 CTE，除非查询确实需要多步骤聚合。

### 规则 4：使用数据库中的实际枚举值
- 查询条件中必须使用捷克语原始值（如 `'VYBER KARTOU'`），不能使用英文翻译（如 `'credit card withdrawal'`）。
- 参考上方的枚举值语义映射表。

### 规则 5：order 表 vs trans 表
- `order` 表存储的是**定期/计划性转账指令**（如每月固定扣款）。
- `trans` 表存储的是**实际交易流水**（每笔实际发生的交易）。
- 当问题提到 "permanent order" 或 "standing order" 时，查 `order` 表。
- 当问题提到 "transaction" 时，查 `trans` 表。
