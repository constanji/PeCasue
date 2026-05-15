# -*- coding: utf-8 -*-
"""
根据「分摊规则.md」与「成本分摊基数+输出模板.xlsx」中三张 **入金笔数 / 出金笔数 / VA个数** 取数
（M=渠道-分行、N=主体.1、J=month、K=BU；与 Excel SUMIFS/固费/VA 口径同说明文档），
**分摊用「成本汇总_*_汇总.xlsx」的「模板口径汇总」为唯一行集权威**（与模板 mapping 中是否列出无关）；mapping 只用于**优先**对应渠道与出摊方式列，无键时按该汇总行分行维度作对应渠道、以总笔数(回退) 出摊。
非 **CITIHK+PPHK** 的汇总行先按 **主体+渠道名称（分行维度）** 合并各桶金额；**CITIHK+PPHK** 仍逐行分账号/聚合，与合并行**同表**，由 **Account** 列展示账号（无账号时为空）。
**CITIHK+PPHK 且汇总行无账号**（inbound / outbound / others）：分 BU 权重 = 全 PPHK+CITI-HK 当月的**该桶 `code` 对应基数量** 减去 汇总中**已列大账号**（大账号 1065249045 不扣、因其走全量笔数行，见 `CITIHK_OUTBOUND_AGGREGATE_ACCOUNT`）的逐大账号**同 `code` 的量和**；和≤0 时整笔进「整体」。
**CITIHK+PPHK+VA 成本**：不区分汇总是否带大账号，一律按 **对应渠道+主体** 在「入金笔数+出金笔数」上的**分行总笔数**（`tot_cnt`）分摊，不按大账号/不按「VA个数」表筛账号；与 `resolve_method` 中 va_m 无关。
按 inbound/outbound/others/VA/收款通道成本 出摊；**五类明细合并为一张「五类成本明细」表**，以 **类型** 列区分；另输出「分摊结果」透视；**整体二次分摊**（FX/收单与余额按笔数）仅在「分摊结果」中并入「合计分摊整体后/整体分摊金额」，不反写合并明细、**不影响**「by BU by 分行维度」的静态全量区与 **C1** 联动上表之口径（上表为引用「成本明细」的公式，与是否另建渠道表无关）。

依赖：pandas、openpyxl、python-calamine（读取含异常筛选器的 xlsx）

副本位于 pipeline-svc，供删除独立 pingpong-master 后仍可使用；BU 列在 ``main()`` 内按 ``TEMPLATE_PATH`` 惰性加载。
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
TEMPLATE_PATH = BASE_DIR / "成本分摊基数+输出模板.xlsx"
# 基数数据路径；pipeline-svc 注入用户上传的基数表时只覆盖此变量，
# TEMPLATE_PATH 仍指向规则库模板（含 inbound 成本 等输出结构 sheet）。
# 默认与 TEMPLATE_PATH 相同（单文件模式）。
BASES_PATH: Path = TEMPLATE_PATH
SUMMARY_PATH = BASE_DIR / "成本汇总_202602_汇总.xlsx"
SUMMARY_SHEET = "模板口径汇总"
OUT_PATH = BASE_DIR / "成本分摊_202602_输出.xlsx"

# 汇总表列名 -> 与明细「类型」一致的分摊桶（顺序与表头一致，勿含「总成本」以免重复）
SUMMARY_BUCKET_COLS: list[tuple[str, str]] = [
    ("inbound", "inbound"),
    ("outbound", "outbound"),
    ("others", "others"),
    ("VA", "VA"),
    ("收款通道成本", "收款通道成本"),
]

PERIOD = 202602
# 分摊规则「特殊处理」全量清单，用于跑完后输出「未命中」项（id -> 与规则表对应说明）
SPECIAL_RULE_CATALOG: list[tuple[str, str]] = [
    ("SR_CITI_SG_OUT_ACCT", "CITI-SG+PPHK outbound 按大账号表→对应 BU（规则表）"),
    ("SR_CITI_SG_OUT_ZT", "CITI-SG+PPHK outbound 无/未匹配账号→整体"),
    ("SR_DBS_HK", "DBS+PPHK+DBS-HK：Tiger 按原权重+余入主站"),
    ("SR_CITI_NZ_PPGT", "CITI+PPGT+CITI-NZ→欧美-Panda"),
    ("SR_CITI_JP_PPJP", "CITI+PPJP+CITI-JP→主站"),
    ("SR_DB_PPHK", "DB+PPHK（非 DBS）各分行→主站"),
    ("SR_LITHIC_PPUS", "Lithic+PPUS→欧美-Tiger"),
    ("SR_ORIENT_PPHK", "Orient+PPHK→主站"),
    ("SR_BAOKIM_FU", "PPGT+BAOKIM/BAOKIM+BAOKIM→福贸"),
    ("SR_QUEEN_BEE", "Queen Bee+PPUS→福贸"),
    ("SR_CHINAUMS", "CHINAUMS+PPHK→主站"),
    ("SR_META_新应用", "METACOMP/HASHKEY/BC+NM 未起量（权重和=0）→新应用"),
    ("SR_BEEPAY_8PCT", "Beepay+PPHK：8%×行成本→欧美-SMB，同额自 APAC-Partnerships 扣"),
]
SPECIAL_RULE_LOG_PATH = BASE_DIR / f"成本分摊_{PERIOD}_特殊规则命中.json"

# 出摊表左侧表头：与业务表 A–F 一致（无 Account 时为 账单渠道、渠道名称、主体、month、<成本列>、分摊方式）
COL_BILL = "账单渠道"
# 与汇总行「渠道名称（分行维度）」一致，供 by BU 动态表与 SUMIFS 条件列
COL_BILL_BR = "渠道名称（分行维度）"
COL_CHANNEL = "渠道名称"
COL_ENTITY = "主体"
COL_MONTH = "month"
COL_ACCOUNT = "Account"
COL_ALLOC = "分摊方式"
# 五类合并明细：类型列取值（与原分表名称一致）、统一金额列名
COL_TYPE = "类型"
COL_AMOUNT = "金额"
SHEET_MERGED_FIVE = "成本明细"
TYPE_INBOUND = "inbound 成本"
TYPE_OUTBOUND = "outbound 成本"
TYPE_OTHERS = "others 成本"
TYPE_RCV = "收款通道成本"
TYPE_VA = "VA成本"
# by BU by 分行维度：C1 下拉的「业务线」与校验版表一致；「APAC」为所有 APAC-* BU 加总；「bu」= 与「合计」同列便于筛选
SHEET_BY_BU_BILL = "by BU by 分行维度"
BY_BU_DROPLIST: tuple[str, ...] = (
    "bu",
    "主站",
    "福贸",
    "银行",
    "APAC-Indonesia",
    "APAC-India",
    "APAC-Japan",
    "APAC-South Korea",
    "APAC-Malaysia",
    "APAC-Philippines",
    "APAC-Pakistani",
    "APAC-Singapore",
    "APAC-Thailand",
    "APAC-Vietnam",
    "APAC-新消费业务",
    "APAC-机构业务",
    "APAC-公共",
    "APAC-Partnerships",
    "欧美-SMB",
    "欧美-Tiger",
    "欧美-Panda",
    "中东",
    "拉美",
    "新应用",
    "FX",
    "整体",
    "APAC",
    "合计",
)
COL_BILL_DIM = "_bill_dim"  # 明细行内：汇总行「渠道名称（分行维度）」原文

# 「分摊结果」透视：与 成本分摊基数+输出模板.xlsx 中结构一致（列宽 36，D 列=成本类型，E=BU）
PIVOT_NCOLS = 36
# 第一段「合计分摊整体后」含 收单；后续 inbound/outbound 等均为 25 行（至「整体」）
PIVOT_BU_GRAND: list[str] = [
    "主站",
    "福贸",
    "银行",
    "APAC-Indonesia",
    "APAC-India",
    "APAC-Japan",
    "APAC-South Korea",
    "APAC-Malaysia",
    "APAC-Philippines",
    "APAC-Pakistani",
    "APAC-Singapore",
    "APAC-Thailand",
    "APAC-Vietnam",
    "APAC-Partnerships",
    "APAC-公共",
    "APAC-新消费业务",
    "APAC-机构业务",
    "欧美-SMB",
    "欧美-Tiger",
    "欧美-Panda",
    "中东",
    "拉美",
    "新应用",
    "FX",
    "收单",
    "整体",
]
PIVOT_BU_STD: list[str] = [bu for bu in PIVOT_BU_GRAND if bu != "收单"]

# 「分摊结果」tiger 行：参考表在 202602 列有单独金额；无核对数时保持 0
PIVOT_TIGER_FEB: float = 0.0
# 「整体分摊金额」各 BU 调整额（全系之和应为 0）；无 JE 时 None 表示全 0（与下方整体二次分摊同时存在时，透视以二次分摊为准）
PIVOT_ZHENGTI_ADJ: dict[str, float] | None = None

# CITIHK+PPHK 下该账号无分账号笔数时，inbound/outbound/others 按「主体+对应渠道」全量基数量分摊（不筛大账号，分摊规则.md）
CITIHK_OUTBOUND_AGGREGATE_ACCOUNT = "1065249045"
# 落在「整体」的金额二次分摊：先预留 FX、收单预估；余额按「所有非整体 BU」的（入金+出金）笔数作分母占比分配（含 FX、收单）
ZHENGTI_RESERVE_FX: float = 4924.0
ZHENGTI_RESERVE_SHOUDAN: float = 4500.0

# 成本汇总「主体」与「VA个数」表「主体.1」名称不一致时，仅 va_cnt 匹配用：键=（_norm_entity_cf(主体), _norm_branch(对应渠道)）→ 基数中主体原样，见 weights_for
VA_CNT_ENTITY_FALLBACK: dict[tuple[str, str], str] = {
    ("brsg", "jpm-sg"): "MANA PAYMENT SG",
}

# 主体=PPHK、分行 CITI-SG 时 outbound 无出金笔数，按 账号 直接对应 BU（分摊规则.md）
CITI_SG_OUTBOUND_BU: dict[str, str] = {
    "7356005": "主站",
    "7356013": "主站",
    "7356048": "主站",
    "7356056": "APAC-公共",
    "7356064": "APAC-公共",
    "7356803": "主站",
    "7356455": "福贸",
    "7356471": "福贸",
    "7356501": "福贸",
    "7356536": "福贸",
    "7356552": "福贸",
    "7356587": "福贸",
    "7356609": "福贸",
    "7356625": "福贸",
    "7356641": "福贸",
    "7356676": "福贸",
    "7356692": "福贸",
    "7356714": "福贸",
    "7356749": "福贸",
    "7356765": "福贸",
    "7356781": "福贸",
    "7356595": "主站",
    "7356722": "欧美-Panda",
    "7356463": "欧美-Panda",
    "7356684": "主站",
}

# Beepay：首摊后 欧美-SMB **固定为** 8%×该行成本（不叠在首摊已分到的 SMB 份额上），Partnerships -= 8%×amt；
#         原首摊落在「欧美-SMB」的 smb0 改记到「主站」使行金额合计仍为 amt
BEEPAY_SMB_RATIO: float = 0.08
BU_SMB: str = "欧美-SMB"
BU_TIGER: str = "欧美-Tiger"
BU_ZHENGTI: str = "整体"
BU_FU: str = "福贸"
BU_PANDA: str = "欧美-Panda"

# CHECK：占位列。收单：整体二次摊中 FX/收单/余额 写入「分摊结果」与 `compute_zhengti_second_split`，
# 不反写五类成本明细则该列恒为 0，故与 CHECK 同逻辑不落表头列（二次摊分母仍单独补全「收单」键，见 main）。
_OMIT_BU_SHEET_COLS: frozenset[str] = frozenset({"收单"})
# 与模板「inbound 成本」第 2 行表头一致（BU 列，含重复「整体」）；在 main() 中按当前 TEMPLATE_PATH 惰性加载，
# 以便 pipeline-svc 在 import 之后注入规则库中的模板路径。
BU_COLS_RAW: list[str] = []


def _ensure_bu_cols_loaded() -> None:
    global BU_COLS_RAW
    if BU_COLS_RAW:
        return
    if not TEMPLATE_PATH.is_file():
        raise FileNotFoundError(TEMPLATE_PATH)
    with pd.ExcelFile(TEMPLATE_PATH, engine="calamine") as xf:
        _hdr = pd.read_excel(xf, sheet_name="inbound 成本", header=None, nrows=3)
    _row2_bu = [str(x).strip() for x in _hdr.iloc[1].tolist()[6:34]]
    BU_COLS_RAW = [
        c for c in _row2_bu if c.casefold() != "check" and c not in _OMIT_BU_SHEET_COLS
    ]


def _bu_values(dist_unique: dict[str, float], *, col_order: list[str]) -> list[float]:
    """与 col_order BU 列顺序对齐的金额向量（重复列名仅首列承接金额，其余为 0）。"""
    seen: set[str] = set()
    out: list[float] = []
    for c in col_order:
        if c in seen:
            out.append(0.0)
        else:
            out.append(float(dist_unique.get(c, 0.0)))
            seen.add(c)
    return out


def _bu_values_extended(
    dist_unique: dict[str, float],
    extra_suffix: list[str],
    *,
    col_order: list[str],
) -> list[float]:
    """模板 BU 列（col_order）+ 扩展 BU 列。"""
    return _bu_values(dist_unique, col_order=col_order) + [
        float(dist_unique.get(e, 0.0)) for e in extra_suffix
    ]


def _ensure_required_bu_tail(bu_sheet_cols_base: list[str], seen: set[str]) -> None:
    """若模板缺少「FX」「整体」，则补在 BU 列末尾。收单不落明细列，由「分摊结果」与二次摊显示。"""
    for name in ("FX", "整体"):
        if name not in seen:
            bu_sheet_cols_base.append(name)
            seen.add(name)


def _template_bu_name_set() -> set[str]:
    s: set[str] = set()
    for c in BU_COLS_RAW:
        if c is None or (isinstance(c, float) and pd.isna(c)):
            continue
        t = str(c).strip()
        if t and t.lower() != "nan":
            s.add(t)
    return s


def _register_extra_bus_from_weights(
    w: pd.Series,
    *,
    template_names: set[str],
    extra_list: list[str],
    extra_seen: set[str],
) -> None:
    for bu in w.index:
        b = str(bu).strip()
        if not b or b.lower() == "nan":
            continue
        if b not in template_names and b not in extra_seen:
            extra_seen.add(b)
            extra_list.append(b)


def _norm_account_key(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return None
    s = s.replace(",", "")
    num = pd.to_numeric(s, errors="coerce")
    if pd.notna(num):
        f = float(num)
        if abs(f - round(f)) < 1e-9:
            return str(int(round(f)))
        return str(f)
    return s


def _norm_branch(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip().replace("_", "-")
    return s.casefold()


def _is_citi_hk_pphk_sheet_row(*, bill_cf: str, corr_display: str, ent_cf: str) -> bool:
    """CITIHK + 对应渠道 CITI-HK + 主体 PPHK：逐行出摊，inbound/outbound/others/收款通道成本 在 Account 列写账号（与输出列 渠道名称/主体 一致）。"""
    if bill_cf != "citihk":
        return False
    if ent_cf != "pphk":
        return False
    return _norm_branch(corr_display) == _norm_branch("CITI-HK")


def _norm_entity_cf(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return str(val).strip().casefold()


def _norm_bill_cf(val) -> str:
    """与明细「分行维度」一致：用于 mapping 键（casefold）。"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return ""
    return s.casefold()


def _row_is_citihk_pphk(row: pd.Series) -> bool:
    return (
        _norm_bill_cf(row.get("渠道名称（分行维度）")) == "citihk"
        and _norm_entity_cf(row.get("主体")) == "pphk"
    )


def _row_citi_sg_pphk(row: pd.Series) -> bool:
    """PPHK 且分行为 CITI-SG 系列（含 CITISG）须逐行保账号，否则 outbound 会误合并（与 citihk+PPHK 同理）。"""
    if _norm_entity_cf(row.get("主体")) != "pphk":
        return False
    disp = _cell_text(row.get("渠道名称（分行维度）", ""))
    bcf = _norm_bill_cf(row.get("渠道名称（分行维度）"))
    return _rule_branch_hits_citi_sg(disp, bcf)


def _row_keep_bill_separate_for_allocation(row: pd.Series) -> bool:
    return _row_is_citihk_pphk(row) or _row_citi_sg_pphk(row)


def _is_db_pphk_not_dbs(bill_cf: str) -> bool:
    """
    规则：渠道为 DB、主体为 PPHK 下各「分行」→主站；与 DBS 区分。
    汇总中常为 DBHK/DB-XX 等；以「db」开头且非「dbs*」即视为 DB 系列（旧逻辑仅 db / db- 会漏掉 dbhk）。
    """
    if not bill_cf:
        return False
    if bill_cf.startswith("dbs"):
        return False
    return bill_cf.startswith("db")


def _rule_branch_hits_citi_sg(bill_display: str, bill_cf: str) -> bool:
    """
    识别「CITI-SG / CITISG / CITI-SG 变体」：汇总里常见无连字 CITISG，bdn 为 citisg 而非 citi-sg。
    """
    for p in (bill_display, str(bill_cf or "")):
        k = _norm_branch(p)
        if not k:
            continue
        if k in ("citi-sg", "citisg"):
            return True
        if re.sub(r"[-_\s]+", "", k) == "citisg":
            return True
    return False


def _rule_branch_hits_citi_nz(
    bill_display: str, bill_cf: str, rec: dict
) -> bool:
    """
    是否可识别为「CITI-NZ」行（与 mapping 的 bill/corr 及汇总列一致，下划线与连字符统一）。
    解决：_norm_bill_cf 不折叠 '_'，citi_nz 与 bdn 的 citi-nz 原先无法对齐。
    """
    for p in (bill_display, bill_cf, str(rec.get("bill", "") or ""), str(rec.get("corr", "") or "")):
        k = _norm_branch(p)
        if not k:
            continue
        if k == "citi-nz" or "citi-nz" in k or k.endswith("citi-nz"):
            return True
    return False


def _rule_branch_hits_citi_jp(
    bill_display: str, bill_cf: str, rec: dict
) -> bool:
    """CITI-JP：同上多源、统一分支写法。"""
    for p in (bill_display, bill_cf, str(rec.get("bill", "") or ""), str(rec.get("corr", "") or "")):
        k = _norm_branch(p)
        if not k:
            continue
        if k == "citi-jp" or "citi-jp" in k or k.endswith("citi-jp"):
            return True
    return False


def _default_mapping_rec(bill_cf: str, bill_display: str) -> dict:
    """
    汇总中出现但「mapping」无键时：以汇总「渠道名称（分行维度）」为账单名；
    对应渠道用于匹配基数表 渠道-分行 —— citihk 与模板约定一致为 CITI-HK，其余用汇总原文。
    各桶分摊方式空则走 resolve_method/weights_for 的「总笔数(回退)」。
    """
    bshow = (bill_display or "").strip() or (bill_cf or "unknown")
    if bill_cf == "citihk":
        corr_s = "CITI-HK"
    else:
        corr_s = bshow
    return {
        "bill": bshow,
        "corr": corr_s,
        "in_m": None,
        "out_m": None,
        "oth_m": None,
        "va_m": None,
        "rcv_m": None,
    }


def _prepare_sm_for_allocation(sm: pd.DataFrame) -> pd.DataFrame:
    """非 CITIHK+PPHK 行按「主体+渠道名称（分行维度）」合并各成本列；CITIHK+PPHK 保持汇总表逐行（含分账号）。"""
    col_entity = "主体"
    col_bill = "渠道名称（分行维度）"
    sum_cols = [c for c, _ in SUMMARY_BUCKET_COLS]
    mask_pphk = sm.apply(_row_keep_bill_separate_for_allocation, axis=1)
    a = sm.loc[mask_pphk].copy()
    b = sm.loc[~mask_pphk]
    if len(b) == 0:
        return a.reset_index(drop=True)
    g = b.groupby([col_entity, col_bill], as_index=False, dropna=False)[sum_cols].sum()
    g["month"] = PERIOD
    if "账号" in b.columns and "账号" not in g.columns:
        g["账号"] = np.nan
    return pd.concat([a, g], ignore_index=True)


def cost_bucket(raw) -> str:
    t = str(raw).strip()
    if not t or t.lower() == "nan":
        return "others"
    if t == "收款通道成本":
        return "收款通道成本"
    tl = t.lower()
    if tl == "inbound":
        return "inbound"
    if tl == "outbound":
        return "outbound"
    if tl == "va":
        return "VA"
    if tl == "others":
        return "others"
    return "others"


def load_mapping() -> dict[str, dict]:
    # 优先从 BASES_PATH 读取 mapping；若 BASES_PATH 不含该 sheet，则回退规则库模板
    _src = TEMPLATE_PATH
    if BASES_PATH != TEMPLATE_PATH and BASES_PATH.is_file():
        try:
            with pd.ExcelFile(BASES_PATH, engine="calamine") as _xf:
                if "mapping" in _xf.sheet_names:
                    _src = BASES_PATH
        except Exception:
            pass
    m = pd.read_excel(_src, sheet_name="mapping", header=None, engine="calamine")
    out: dict[str, dict] = {}
    for i in range(2, len(m)):
        row = m.iloc[i]
        bill = row[5]
        if pd.isna(bill) or str(bill).strip() == "" or str(bill).lower() == "nan":
            continue
        key = str(bill).strip().casefold()
        corr = row[6]
        corr_s = str(corr).strip() if pd.notna(corr) else str(bill).strip()
        out[key] = {
            "bill": str(bill).strip(),
            "corr": corr_s,
            "in_m": row[7],
            "out_m": row[8],
            "oth_m": row[9],
            "va_m": row[10],
            # L 列：收款通道成本分摊方式（与模板 F–L 红框一致）
            "rcv_m": row[11] if m.shape[1] > 11 else None,
        }
    return out


def _cell_text(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return str(x).strip()


def parse_method_code(m) -> str | None:
    s = _cell_text(m)
    if not s or s.lower() == "nan":
        return None
    if "固费" in s:
        return "fixed_global"
    if "VA个数" in s or s == "VA个数":
        return "va_cnt"
    if "入金笔数" in s:
        return "in_cnt"
    if "出金笔数" in s:
        return "out_cnt"
    if "总笔数" in s:
        return "tot_cnt"
    if "入金交易量" in s:
        return "in_vol"
    if "出金交易量" in s:
        return "out_vol"
    if "总交易量" in s:
        return "tot_vol"
    return None


def resolve_method(bucket: str, rec: dict) -> str | None:
    if bucket == "inbound":
        for k in ("in_m", "oth_m", "out_m"):
            c = parse_method_code(rec.get(k))
            if c:
                return c
    elif bucket == "outbound":
        for k in ("out_m", "oth_m", "in_m"):
            c = parse_method_code(rec.get(k))
            if c:
                return c
    elif bucket == "收款通道成本":
        for k in ("rcv_m", "oth_m", "in_m", "out_m"):
            c = parse_method_code(rec.get(k))
            if c:
                return c
    elif bucket == "others":
        for k in ("oth_m", "in_m", "out_m"):
            c = parse_method_code(rec.get(k))
            if c:
                return c
    else:  # VA
        for k in ("va_m", "oth_m", "in_m"):
            c = parse_method_code(rec.get(k))
            if c:
                return c
    return None


def _standardize_inout_base(df: pd.DataFrame, *, outbound: bool) -> pd.DataFrame:
    """
    与「入金笔数 / 出金笔数」sheet 表头一致（第 2 行为字段名）：
    A 月份, B 主体, … G 笔数, H 交易量, … J month, K BU, L 渠道, M 渠道-分行, N 主体.1
    与 Excel SUMIFS(…, M,$B3, N,$C3, J,$D3) 列对应。
    """
    want = [
        "月份",
        "主体",
        "渠道名称",
        "大账号",
        "业务系统",
        "客户kyc国家",
        "出金笔数" if outbound else "入金笔数",
        "出金交易量" if outbound else "入金交易量",
        "最终bu",
        "month",
        "BU",
        "渠道",
        "渠道-分行",
        "主体.1",
    ]
    n = min(14, df.shape[1])
    out = df.iloc[:, :n].copy()
    out.columns = want[:n]
    if "主体.1" not in out.columns and "主体" in out.columns:
        out["主体.1"] = out["主体"]
    bc = "出金笔数" if outbound else "入金笔数"
    vc = "出金交易量" if outbound else "入金交易量"
    out[bc] = pd.to_numeric(out[bc], errors="coerce").fillna(0.0)
    out[vc] = pd.to_numeric(out[vc], errors="coerce").fillna(0.0)
    out["month"] = pd.to_numeric(out["month"], errors="coerce")
    return out


def _standardize_va_base(va_raw: pd.DataFrame, month: int) -> pd.DataFrame:
    """
    「VA个数」sheet：BU、有效 VA 数、month、渠道-分行、主体；
    与入金/出金一致，对 C 列主体按 SUMIFS 语义使用「主体.1」列（本表常仅有一列主体，则复制为 主体.1）。

    兼容两种布局：
      • 标准（10 列）：col 0 = 空白占位, col 3=BU, 4=VA数, 5=month, 8=渠道-分行, 9=主体
      • 紧凑（9 列）：无前置空白列，col 2=BU, 3=VA数, 4=month, 7=渠道-分行, 8=主体
    """
    ncols = va_raw.shape[1]
    if ncols >= 10:
        # 标准格式（含前置空白列）
        o = 0
    elif ncols >= 9:
        # 紧凑格式（缺前置空白列，整体左移 1）
        o = -1
    else:
        raise RuntimeError("VA个数 表列数不足，无法对齐模板")
    out = pd.DataFrame(
        {
            "BU": va_raw.iloc[:, 3 + o],
            "va_cnt": pd.to_numeric(va_raw.iloc[:, 4 + o], errors="coerce").fillna(0.0),
            "month": pd.to_numeric(va_raw.iloc[:, 5 + o], errors="coerce"),
            "渠道-分行": va_raw.iloc[:, 8 + o].astype(str).str.strip(),
            "主体": va_raw.iloc[:, 9 + o].astype(str).str.strip(),
        }
    )
    out["主体.1"] = out["主体"]
    return out[out["month"] == month].copy()


def _entity_match_series(df: pd.DataFrame) -> pd.Series:
    """与 Excel SUMIFS(入金/出金!$N:$N,$C3) 一致：优先 N 列 主体.1，空则 B 列 主体。"""
    if "主体.1" not in df.columns:
        return df["主体"].map(_norm_entity_cf)
    ext = df["主体.1"].map(_norm_entity_cf)
    main = df["主体"].map(_norm_entity_cf)
    has_ext = df["主体.1"].map(lambda x: bool(_cell_text(x)))
    return ext.where(has_ext, main)


def method_label(code: str | None, rec: dict, bucket: str) -> str:
    if code == "fixed_global":
        return "固费分摊"
    order = []
    if bucket == "inbound":
        order = ["in_m", "oth_m", "out_m"]
    elif bucket == "outbound":
        order = ["out_m", "oth_m", "in_m"]
    elif bucket == "收款通道成本":
        order = ["rcv_m", "oth_m", "in_m", "out_m"]
    elif bucket == "others":
        order = ["oth_m", "in_m", "out_m"]
    else:
        order = ["va_m", "oth_m", "in_m"]
    for k in order:
        s = _cell_text(rec.get(k))
        if s and parse_method_code(rec.get(k)) == code:
            return s
    return code or ""


def load_bases(month: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.Series]:
    """从模板读取三张分摊基数表：入金笔数、出金笔数、VA个数（原始 VA 用于标准化）。

    若 BASES_PATH 与 TEMPLATE_PATH 不同且文件存在，则优先从 BASES_PATH 读取基数数据；
    这允许 pipeline-svc 注入用户上传的基数表，同时保持 TEMPLATE_PATH 指向完整输出模板。
    """
    _src = BASES_PATH if BASES_PATH != TEMPLATE_PATH and BASES_PATH.is_file() else TEMPLATE_PATH
    fin_raw = pd.read_excel(_src, sheet_name="入金笔数", header=1, engine="calamine")
    fout_raw = pd.read_excel(_src, sheet_name="出金笔数", header=1, engine="calamine")
    fin = _standardize_inout_base(fin_raw, outbound=False)
    fout = _standardize_inout_base(fout_raw, outbound=True)
    fin = fin[fin["month"] == month].copy()
    fout = fout[fout["month"] == month].copy()

    va_raw = pd.read_excel(_src, sheet_name="VA个数", header=1, engine="calamine")

    in_sum = fin.groupby("BU")["入金笔数"].sum()
    out_sum = fout.groupby("BU")["出金笔数"].sum()
    global_tot = in_sum.add(out_sum, fill_value=0.0)

    return fin, fout, va_raw, global_tot


def weights_for(
    code: str | None,
    *,
    fin: pd.DataFrame,
    fout: pd.DataFrame,
    va: pd.DataFrame,
    global_tot: pd.Series,
    month: int,
    entity_cf: str,
    corr_nf: str,
    account_key: str | None,
) -> tuple[pd.Series, str]:
    """
    返回 (BU->权重, 分摊方式展示文案)。
    入金/出金：与 IFS+SUMIFS 一致 — M=渠道-分行↔出摊渠道名称，N=主体.1↔出摊主体，J=month↔出摊月；
    固费：仅按 J=month 汇总全表笔数（global_tot）。
    VA：VA个数 表按 渠道-分行、主体.1 语义、month 筛选后按 BU 汇总 va_cnt。
    """
    if code == "fixed_global":
        s = global_tot.astype(float)
        return s, "固费分摊"

    def mask_inout(df: pd.DataFrame) -> pd.Series:
        m = _entity_match_series(df) == entity_cf
        m &= df["渠道-分行"].map(_norm_branch) == corr_nf
        m &= pd.to_numeric(df["month"], errors="coerce") == int(month)
        if account_key is not None:
            m &= df["大账号"].map(_norm_account_key) == account_key
        return m

    mi = mask_inout(fin)
    mo = mask_inout(fout)

    if code == "in_cnt":
        w = fin.loc[mi].groupby("BU")["入金笔数"].sum()
        return w.astype(float), "入金笔数"
    if code == "out_cnt":
        w = fout.loc[mo].groupby("BU")["出金笔数"].sum()
        return w.astype(float), "出金笔数"
    if code == "tot_cnt":
        wi = fin.loc[mi].groupby("BU")["入金笔数"].sum()
        wo = fout.loc[mo].groupby("BU")["出金笔数"].sum()
        w = wi.add(wo, fill_value=0.0).astype(float)
        return w, "总笔数"
    if code == "in_vol":
        w = fin.loc[mi].groupby("BU")["入金交易量"].sum()
        return w.astype(float), "入金交易量"
    if code == "out_vol":
        w = fout.loc[mo].groupby("BU")["出金交易量"].sum()
        return w.astype(float), "出金交易量"
    if code == "tot_vol":
        wi = fin.loc[mi].groupby("BU")["入金交易量"].sum()
        wo = fout.loc[mo].groupby("BU")["出金交易量"].sum()
        w = wi.add(wo, fill_value=0.0).astype(float)
        return w, "总交易量"
    if code == "va_cnt":
        def _va_bu_by_entity(ent_f: str) -> pd.Series:
            mv0 = (
                (_entity_match_series(va) == ent_f)
                & (va["渠道-分行"].map(_norm_branch) == corr_nf)
                & (pd.to_numeric(va["month"], errors="coerce") == int(month))
            )
            if account_key is not None and "大账号" in va.columns:
                mv0 &= va["大账号"].map(_norm_account_key) == account_key
            return va.loc[mv0].groupby("BU")["va_cnt"].sum()
        w = _va_bu_by_entity(entity_cf)
        how_va = "VA个数"
        if float(w.sum()) <= 0.0:
            fb = VA_CNT_ENTITY_FALLBACK.get((entity_cf, corr_nf))
            if fb:
                ent_alt = _norm_entity_cf(fb)
                w2 = _va_bu_by_entity(ent_alt)
                if float(w2.sum()) > 0.0:
                    w = w2
                    how_va = f"VA个数（基数主体回退{fb!r}）"
        return w.astype(float), how_va

    wi = fin.loc[mi].groupby("BU")["入金笔数"].sum()
    wo = fout.loc[mo].groupby("BU")["出金笔数"].sum()
    w = wi.add(wo, fill_value=0.0).astype(float)
    return w, "总笔数(回退)"


def _citihk_pphk_listed_account_keys_from_summary(sm: pd.DataFrame) -> set[str]:
    """
    汇总中 CITIHK+PPHK 行若「账号」非空，其规范化大账号即「已列大账号」——
    无账号行扣减 有账号 部分时，只扣这些 key（1065249045 单独用全量笔数行，不扣逐账号笔数，见 CITIHK_OUTBOUND_AGGREGATE_ACCOUNT）。
    """
    out: set[str] = set()
    if sm is None or len(sm) == 0 or "账号" not in sm.columns:
        return out
    for _, row in sm.iterrows():
        if _norm_bill_cf(row.get("渠道名称（分行维度）")) != "citihk":
            continue
        if _norm_entity_cf(row.get("主体")) != "pphk":
            continue
        k = _norm_account_key(row.get("账号"))
        if k:
            out.add(k)
    return out


def weights_citihk_pphk_no_acct_residual(
    code: str | None,
    *,
    fin: pd.DataFrame,
    fout: pd.DataFrame,
    va: pd.DataFrame,
    global_tot: pd.Series,
    month: int,
    entity_cf: str,
    corr_nf: str,
    listed_accounts: set[str],
) -> tuple[pd.Series, str]:
    """
    账单 CITIHK、主体 PPHK、汇总行「无账号」时：权重按
    各 BU 上（全 PPHK+CITI-HK 在当月、按 code 的笔数/交易量向量 −
    对「已列大账号」逐账号按同 code 求得的向量和），再 clip≥0、分母为 0 时整笔进「整体」。
    出金/入金/总笔数等与 resolve_method+code 一致；「1065249045」行在基数上走全量，不参与扣减和。
    """
    w_full, h0 = weights_for(
        code,
        fin=fin,
        fout=fout,
        va=va,
        global_tot=global_tot,
        month=month,
        entity_cf=entity_cf,
        corr_nf=corr_nf,
        account_key=None,
    )
    w_full = w_full.astype(float).fillna(0.0)
    sub_keys = {a for a in listed_accounts if a and a != CITIHK_OUTBOUND_AGGREGATE_ACCOUNT}
    if not sub_keys:
        return w_full, f"{h0}（CITIHK+PPHK 无账号：无已列大账号，同全量）"
    w_sub: pd.Series | None = None
    for a in sub_keys:
        wa, _ = weights_for(
            code,
            fin=fin,
            fout=fout,
            va=va,
            global_tot=global_tot,
            month=month,
            entity_cf=entity_cf,
            corr_nf=corr_nf,
            account_key=a,
        )
        wa = wa.astype(float).fillna(0.0)
        w_sub = wa if w_sub is None else w_sub.add(wa, fill_value=0.0)
    if w_sub is None:
        w_sub = pd.Series(dtype=float)
    w_res = w_full.subtract(w_sub, fill_value=0.0)
    w_res = w_res.fillna(0.0)
    w_res[w_res < 0] = 0.0
    tot = float(w_res.sum())
    if tot <= 1e-12:
        s = _wseries_single(BU_ZHENGTI)
        return s, f"{h0}（CITIHK+PPHK 无账号：全量−已列账号≤0→整体）"
    return w_res, f"{h0}（CITIHK+PPHK 无账号：全量−已列大账号分账号笔数/量）"


def distribute(cost: float, weights: pd.Series, bu_keys: list[str]) -> dict[str, float]:
    w = weights.astype(float).fillna(0.0)
    total = float(w.sum())
    out: dict[str, float] = {c: 0.0 for c in bu_keys}
    if cost == 0:
        return out
    if total <= 0:
        if "整体" not in out:
            raise RuntimeError("内部错误：BU 列中仍无「整体」，请检查 _ensure_required_bu_tail 是否已执行。")
        out["整体"] = cost
        return out
    for bu, wt in w.items():
        b = str(bu).strip()
        if b not in out:
            raise RuntimeError(
                f"分摊权重含 BU「{b}」，但未纳入 bu_keys（请检查扩展 BU 收集是否先于分摊）。"
            )
        out[b] += cost * float(wt) / total
    return out


def _wseries_single(bu: str) -> pd.Series:
    return pd.Series([1.0], index=[str(bu).strip()])


def pre_special_alloc_weights(
    *,
    bill_cf: str,
    bill_display: str,
    ent_cf: str,
    bucket: str,
    account_key: str | None,
    rec: dict,
    code: str | None,
    fin: pd.DataFrame,
    fout: pd.DataFrame,
    va: pd.DataFrame,
    global_tot: pd.Series,
    month: int,
) -> tuple[pd.Series, str, str] | None:
    """
    若命中 分摊规则.md 中「特殊处理」，返回 (w_series, 说明, rule_id)；未命中返回 None 走 weights_for。
    rule_id 与 SPECIAL_RULE_CATALOG 中各项对应，供命中统计与「未生效」报告。
    """
    bd = (bill_display or "").strip()
    bdn = _norm_branch(bd)
    corr_nf = _norm_branch(rec.get("corr", ""))

    def _w_for_special() -> tuple[pd.Series, str]:
        return weights_for(
            code,
            fin=fin,
            fout=fout,
            va=va,
            global_tot=global_tot,
            month=month,
            entity_cf=ent_cf,
            corr_nf=corr_nf,
            account_key=None,
        )

    # 1) CITI-SG + PPHK：outbound 按账号→BU，否则→整体（含 CITISG 等无连字写法，见 _rule_branch_hits_citi_sg）
    if ent_cf == "pphk" and bucket == "outbound" and _rule_branch_hits_citi_sg(
        bd, bill_cf
    ):
        k = _norm_account_key(account_key)
        if k and k in CITI_SG_OUTBOUND_BU:
            bname = CITI_SG_OUTBOUND_BU[k]
            return _wseries_single(bname), f"CITI-SG 出金按账号→{bname}({k})", "SR_CITI_SG_OUT_ACCT"
        return _wseries_single(BU_ZHENGTI), "CITI-SG 出金未匹配账号→整体", "SR_CITI_SG_OUT_ZT"

    # 2) DBS + PPHK + DBS-HK：「欧美-Tiger」取原全表权重中 Tiger 的占比，其余 100% 进「主站」
    is_dbs_hk_pphk = ent_cf == "pphk" and (
        bdn in ("dbs-hk",)
        or re.search(r"dbs-?hk", bdn) is not None
        or re.search(r"dbs-?hk", bd, re.I) is not None
    )
    if is_dbs_hk_pphk:
        w0, h0 = _w_for_special()
        w0 = w0.astype(float)
        tsum = float(w0.sum())
        if tsum > 0.0:
            tw = float(w0.get(BU_TIGER, 0.0) or 0.0) / tsum
            s = pd.Series({BU_TIGER: tw, "主站": 1.0 - tw}, dtype=float)
        else:
            s = _wseries_single("主站")
        return s, f"{h0}；DBS-HK: Tiger 按原权重比+余入主站", "SR_DBS_HK"

    # 3) CITI+PPGT+CITI-NZ、CITI+PPJP+CITI-JP（分行多源+下划线/连字符合一，优先于 4)）
    if ent_cf == "ppgt" and _rule_branch_hits_citi_nz(
        bill_display, bill_cf, rec
    ):
        return _wseries_single(BU_PANDA), "CITI+PPGT+CITI-NZ→欧美-Panda", "SR_CITI_NZ_PPGT"

    if ent_cf == "ppjp" and _rule_branch_hits_citi_jp(bill_display, bill_cf, rec):
        return _wseries_single("主站"), "CITI+PPJP+CITI-JP→主站", "SR_CITI_JP_PPJP"

    # 4) DB+PPHK（与 DBS 已区分；db / db- 等，不含 dbs 前缀行）
    if ent_cf == "pphk" and _is_db_pphk_not_dbs(bill_cf):
        return _wseries_single("主站"), "DB+PPHK：统一主站", "SR_DB_PPHK"

    # Queen Bee+PPUS（与 Lithic 同为 PPUS，先判 Queen Bee 分行名）
    if ent_cf == "ppus" and (
        bill_cf == "queen bee" or re.search("queen.*bee|queenbee", bdn, re.I) is not None
    ):
        return _wseries_single(BU_FU), "Queen Bee+PPUS→福贸", "SR_QUEEN_BEE"

    if ent_cf == "ppus" and (bdn in ("lithic",) or bill_cf == "lithic"):
        return _wseries_single(BU_TIGER), "Lithic+PPUS→欧美-Tiger", "SR_LITHIC_PPUS"

    if ent_cf == "pphk" and (bdn in ("orient",) or bill_cf == "orient"):
        return _wseries_single("主站"), "Orient+PPHK→主站", "SR_ORIENT_PPHK"

    if ent_cf == "baokim" and ("baokim" in bdn or bdn in ("baokim",)):
        return _wseries_single(BU_FU), "BAOKIM→福贸", "SR_BAOKIM_FU"
    if ent_cf == "ppgt" and bdn in ("baokim",) and "baokim" in str(rec.get("bill", "")).casefold():
        return _wseries_single(BU_FU), "PPGT+BAOKIM→福贸", "SR_BAOKIM_FU"

    if ent_cf == "pphk" and (bill_cf == "chinaums" or "chinaums" in bdn):
        return _wseries_single("主站"), "CHINAUMS+PPHK→主站", "SR_CHINAUMS"

    # METACOMP/HASHKEY/BC+NM 未起量：基数权重和为 0 时整笔入 新应用
    may_meta = re.search("metacomp|hashkey", bd, re.I) is not None
    may_bc_nm = ent_cf == "nm" and (bdn in ("bc",) or bdn.startswith("bc-") or re.search(r"^bc[/\-_]", bdn) is not None)
    if may_meta or may_bc_nm:
        w_try, h_try = _w_for_special()
        w_try = w_try.astype(float)
        if float(w_try.sum()) <= 0.0:
            return _wseries_single("新应用"), f"{h_try}；未起量→新应用", "SR_META_新应用"

    return None


def apply_beepay_smb8_to_smb_from_partnership(
    dist: dict[str, float], *, cost: float
) -> dict[str, float]:
    """
    Beepay 行（PPHK+Beepay）：在首遍 distribute 后
    ① 本行「欧美-SMB」= 8%×本行成本（与 =E*8% 一致），不能等于「首摊分到 SMB 的份额 +8%」；
    ② APAC-Partnerships 扣 8%×本行成本（同 t）；
    ③ 首摊若已向「欧美-SMB」分了 smb0，改记到「主站」，保证各列之和仍为本行 cost。
    """
    bp = "APAC-Partnerships"
    t = min(cost * float(BEEPAY_SMB_RATIO), max(0.0, cost))
    print("beepay_smb8_to_smb_from_partnership", t)
    if t <= 0.0:
        return dist
    p0 = max(0.0, float(dist.get(bp, 0.0)))
    smb0 = max(0.0, float(dist.get(BU_SMB, 0.0)))
    dist[bp] = p0 - t
    dist[BU_SMB] = t
    if smb0:
        zhu = "主站"
        dist[zhu] = dist.get(zhu, 0.0) + smb0
    return dist


def _sum_overall_from_detail_rows(
    row_lists: list[list[dict]],
    bu_sheet_cols: list[str],
) -> float:
    try:
        ix = bu_sheet_cols.index("整体")
    except ValueError:
        return 0.0
    s = 0.0
    for rows in row_lists:
        for r in rows:
            vec = r.get("_bu")
            if vec is None or len(vec) <= ix:
                continue
            s += float(vec[ix])
    return s


def compute_zhengti_second_split(
    G: float,
    *,
    full_bu_keys: list[str],
    global_tot: pd.Series,
    fx_fixed: float,
    shoudan_fixed: float,
) -> dict[str, float]:
    """
    整体列合计 G：先预留 FX、收单预估（4924/4500）；余额 R=G-预留 按分摊规则.md：
    R ×（某 BU 出入金笔数和）÷（所有非「整体」BU 的出入金笔数和），含 FX、收单在内。
    若 G 小于两笔预留之和，则整笔 G 按 4924:4500 比例仅在 FX 与收单之间拆分。
    """
    extra: defaultdict[str, float] = defaultdict(float)
    if G <= 0:
        return {}
    tot_fix = float(fx_fixed) + float(shoudan_fixed)
    if G <= tot_fix and tot_fix > 0:
        extra["FX"] = G * float(fx_fixed) / tot_fix
        extra["收单"] = G - extra["FX"]
        return dict(extra)
    extra["FX"] = float(fx_fixed)
    extra["收单"] = float(shoudan_fixed)
    r_pool = G - float(fx_fixed) - float(shoudan_fixed)
    non_zt = [
        b
        for b in full_bu_keys
        if b != "整体" and str(b).strip() and str(b).lower() != "nan"
    ]
    s_den = sum(float(global_tot.get(b, 0.0)) for b in non_zt)
    if r_pool > 0 and s_den > 0:
        for b in non_zt:
            extra[b] += r_pool * float(global_tot.get(b, 0.0)) / s_den
    elif r_pool > 0 and non_zt:
        eq = r_pool / len(non_zt)
        for b in non_zt:
            extra[b] += eq
    elif r_pool > 0 and tot_fix > 0:
        extra["FX"] += r_pool * float(fx_fixed) / tot_fix
        extra["收单"] += r_pool * float(shoudan_fixed) / tot_fix
    return dict(extra)


def build_sheet_rows(
    rows: list[dict],
    cost_col: str,
    *,
    with_account: bool,
    bu_sheet_cols: list[str],
) -> pd.DataFrame:
    cols_meta = [COL_BILL, COL_BILL_BR, COL_CHANNEL, COL_ENTITY, COL_MONTH]
    if with_account:
        cols_meta = [COL_BILL, COL_BILL_BR, COL_CHANNEL, COL_ENTITY, COL_MONTH, COL_ACCOUNT]
    cols_meta += [cost_col, COL_ALLOC]
    full_cols = cols_meta + bu_sheet_cols + ["合计"]
    if not rows:
        return pd.DataFrame(columns=full_cols)
    meta = pd.DataFrame(rows)
    bu = pd.DataFrame(meta.pop("_bu").tolist(), columns=bu_sheet_cols)
    apac_sub = [c for c in bu_sheet_cols if str(c).strip().startswith("APAC-")]
    if "APAC" in bu_sheet_cols and apac_sub:
        sub = bu[apac_sub].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        bu["APAC"] = sub.sum(axis=1)
    meta = meta[cols_meta]
    df = pd.concat([meta, bu], axis=1)
    if "APAC" in bu_sheet_cols:
        bu_sum = bu[[c for c in bu_sheet_cols if str(c).strip() != "APAC"]]
        df["合计"] = bu_sum.apply(pd.to_numeric, errors="coerce").fillna(0.0).sum(axis=1)
    else:
        df["合计"] = bu.sum(axis=1)
    return df


def merge_five_cost_dataframes(
    df_in: pd.DataFrame,
    df_ou: pd.DataFrame,
    df_ot: pd.DataFrame,
    df_rcv: pd.DataFrame,
    df_va: pd.DataFrame,
    *,
    bu_sheet_cols: list[str],
) -> pd.DataFrame:
    """
    将五类明细合并为一张表：统一 **金额** 列，**类型** 列取值为原分表名；VA 无 Account 时补空串。
    列顺序：类型, 账单渠道, 渠道名称, 主体, month, Account, 金额, 分摊方式, 各 BU…, 合计。
    """
    specs: list[tuple[pd.DataFrame, str, str]] = [
        (df_in, TYPE_INBOUND, "inbound成本"),
        (df_ou, TYPE_OUTBOUND, "outbound成本"),
        (df_ot, TYPE_OTHERS, "others 成本"),
        (df_rcv, TYPE_RCV, "收款通道成本"),
        (df_va, TYPE_VA, "VA成本"),
    ]
    pieces: list[pd.DataFrame] = []
    for df, type_label, cost_key in specs:
        x = df.copy()
        if cost_key in x.columns:
            x = x.rename(columns={cost_key: COL_AMOUNT})
        x[COL_TYPE] = type_label
        if COL_ACCOUNT not in x.columns:
            x[COL_ACCOUNT] = ""
        else:
            x[COL_ACCOUNT] = (
                x[COL_ACCOUNT].apply(
                    lambda v: "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v).strip()
                )
            )
        pieces.append(x)
    out = pd.concat(pieces, ignore_index=True)
    front = [
        COL_TYPE,
        COL_BILL,
        COL_BILL_BR,
        COL_CHANNEL,
        COL_ENTITY,
        COL_MONTH,
        COL_ACCOUNT,
        COL_AMOUNT,
        COL_ALLOC,
    ]
    rest = list(bu_sheet_cols) + ["合计"]
    cols = [c for c in front + rest if c in out.columns]
    return out[cols]


def _sum_bu_from_type(
    df: pd.DataFrame,
    type_value: str,
    bu_names: list[str],
) -> dict[str, float]:
    if df is None or len(df) == 0 or COL_TYPE not in df.columns:
        return {bu: 0.0 for bu in bu_names}
    sub = df[df[COL_TYPE].astype(str).str.strip() == str(type_value).strip()]
    return _sum_bu_from_frames([sub], bu_names)


def _sum_bu_from_frames(frames: list[pd.DataFrame], bu_names: list[str]) -> dict[str, float]:
    acc = {bu: 0.0 for bu in bu_names}
    for df in frames:
        if df is None or len(df) == 0:
            continue
        for bu in bu_names:
            if bu not in df.columns:
                continue
            col = df[bu]
            if isinstance(col, pd.DataFrame):
                acc[bu] += float(col.apply(pd.to_numeric, errors="coerce").fillna(0.0).to_numpy().sum())
            else:
                acc[bu] += float(pd.to_numeric(col, errors="coerce").fillna(0.0).sum())
    return acc


def _pivot_month_col_index(period: int) -> int:
    return 5 + (int(period) - 202601)


def _pivot_header_row() -> list:
    r = [None] * PIVOT_NCOLS
    r[3] = "成本类型"
    r[4] = "BU"
    for i in range(12):
        r[5 + i] = float(202601 + i)
    r[17] = "合计"
    return r


def _pivot_data_row(cost_type: str, bu: str, value: float, mi: int) -> list:
    row = [None] * PIVOT_NCOLS
    row[3] = cost_type
    row[4] = bu
    v = float(value)
    row[mi] = v
    row[17] = v
    return row


def _pivot_footer_sum(block_rows: list[list], mi: int) -> list:
    t = [None] * PIVOT_NCOLS
    t[4] = "合计"
    s = 0.0
    for r in block_rows:
        if r[mi] is not None:
            s += float(r[mi])
    t[mi] = s
    t[17] = s
    return t


def _pivot_tiger_row(mi: int, feb: float) -> list:
    """参考窄表：tiger 占 BU 列，金额在当月列；合计列可空。"""
    r = [None] * PIVOT_NCOLS
    r[4] = "tiger分摊差异金额"
    fv = float(feb)
    if fv != 0.0:
        r[mi] = fv
    return r


def build_fentan_pivot(
    sheets: dict[str, pd.DataFrame],
    period: int,
    *,
    extra_bus: list[str] | None = None,
    zt_second_split: dict[str, float] | None = None,
) -> pd.DataFrame:
    """
    生成与模板「分摊结果」透视结构一致的宽表（36 列，前 3 行留空）。
    「五类成本明细」合并表仅含第一遍分摊；整体二次分摊不反写该表，只在透视中合并：
    若提供 zt_second_split：「合计分摊整体后」= 五类合计（「整体」列视作 0，其余列保留第一摊）+ 整体二次摊 zt。
    「整体分摊金额」行仍为 zt。否则「合计分摊整体后」= 五类合计 + PIVOT_ZHENGTI_ADJ。
    extra_bus：基数表中出现、模板 BU 列未列名的 BU，透视中接在固定 BU 段之后展示。
    """
    ext = list(extra_bus or [])
    all_grand = list(PIVOT_BU_GRAND) + ext
    std_plus = list(PIVOT_BU_STD) + ext
    mi = _pivot_month_col_index(period)
    m5 = sheets[SHEET_MERGED_FIVE]
    v_in = _sum_bu_from_type(m5, TYPE_INBOUND, all_grand)
    v_ou = _sum_bu_from_type(m5, TYPE_OUTBOUND, all_grand)
    v_ot = _sum_bu_from_type(m5, TYPE_OTHERS, all_grand)
    v_va = _sum_bu_from_type(m5, TYPE_VA, all_grand)
    v_rcv = _sum_bu_from_type(m5, TYPE_RCV, all_grand)
    v_five = {
        bu: v_in.get(bu, 0.0) + v_ou.get(bu, 0.0) + v_ot.get(bu, 0.0) + v_va.get(bu, 0.0) + v_rcv.get(bu, 0.0)
        for bu in all_grand
    }
    use_second = zt_second_split is not None
    zt_map_grand: dict[str, float] = {bu: 0.0 for bu in all_grand}
    if not use_second:
        zt_legacy = (
            {bu: float(PIVOT_ZHENGTI_ADJ.get(bu, 0.0)) for bu in PIVOT_BU_STD}
            if PIVOT_ZHENGTI_ADJ is not None
            else {bu: 0.0 for bu in PIVOT_BU_STD}
        )
        for bu in all_grand:
            zt_map_grand[bu] = float(zt_legacy.get(bu, 0.0))

    rows: list[list] = []
    rows.extend([[None] * PIVOT_NCOLS for _ in range(3)])
    rows.append(_pivot_header_row())

    blk_grand: list[list] = []
    for bu in all_grand:
        if use_second and zt_second_split is not None:
            v_part = 0.0 if str(bu).strip() == "整体" else float(v_five.get(bu, 0.0))
            val = v_part + float(zt_second_split.get(bu, 0.0))
        else:
            heji_bu = v_five.get(bu, 0.0)
            zt_bu = zt_map_grand.get(bu, 0.0)
            val = heji_bu + zt_bu
        row = _pivot_data_row("合计分摊整体后", bu, val, mi)
        rows.append(row)
        blk_grand.append(row)
    rows.append(_pivot_footer_sum(blk_grand, mi))

    rows.append(_pivot_tiger_row(mi, PIVOT_TIGER_FEB))
    rows.append([None] * PIVOT_NCOLS)

    rows.append(_pivot_header_row())
    blk_zt: list[list] = []
    zt_display_keys = all_grand
    for bu in zt_display_keys:
        zv = float(zt_second_split.get(bu, 0.0)) if use_second else float(zt_map_grand.get(bu, 0.0))
        row = _pivot_data_row("整体分摊金额", bu, zv, mi)
        rows.append(row)
        blk_zt.append(row)
    rows.append(_pivot_footer_sum(blk_zt, mi))

    rows.append([None] * PIVOT_NCOLS)
    rows.append(_pivot_header_row())
    blk_heji: list[list] = []
    for bu in std_plus:
        row = _pivot_data_row("合计", bu, v_five.get(bu, 0.0), mi)
        rows.append(row)
        blk_heji.append(row)
    rows.append(_pivot_footer_sum(blk_heji, mi))

    for ct_key, label in (
        ("in", "inbound"),
        ("ou", "outbound"),
        ("ot", "others"),
        ("va", "VA成本"),
        ("rcv", "收款通道成本"),
    ):
        rows.append([None] * PIVOT_NCOLS)
        rows.append(_pivot_header_row())
        if ct_key == "in":
            v = v_in
        elif ct_key == "ou":
            v = v_ou
        elif ct_key == "ot":
            v = v_ot
        elif ct_key == "va":
            v = v_va
        else:
            v = v_rcv
        blk: list[list] = []
        for bu in std_plus:
            row = _pivot_data_row(label, bu, v.get(bu, 0.0), mi)
            rows.append(row)
            blk.append(row)
        rows.append(_pivot_footer_sum(blk, mi))

    for r in rows:
        if len(r) < PIVOT_NCOLS:
            r.extend([None] * (PIVOT_NCOLS - len(r)))
        if r[PIVOT_NCOLS - 1] is None:
            r[PIVOT_NCOLS - 1] = 0.0
    return pd.DataFrame(rows, columns=list(range(PIVOT_NCOLS)))


def build_bu_by_bill_dim_df(
    bu_bill_agg: dict[tuple[str, str], float] | defaultdict[tuple[str, str], float],
    *,
    bu_order: list[str],
) -> pd.DataFrame:
    """
    行=汇总「渠道名称（分行维度）」去重，列=业务线：单 BU 列取 (BU, 分行) 加总；
    「APAC」= 本行中所有以 APAC- 开头的 BU 之和；「bu」「合计」= 行总（全 BU 之和，含收单/整体等）。
    """
    bill_dims = sorted(
        {bd for (_bu, bd) in bu_bill_agg.keys() if str(bd).strip()}, key=lambda x: str(x)
    )
    if not bill_dims:
        return pd.DataFrame(
            [{"分行维度": "合计", **{lab: 0.0 for lab in BY_BU_DROPLIST}}]
        )
    rows: list[dict] = []
    for bd in bill_dims:
        line_total = 0.0
        apac_sum = 0.0
        for bu in bu_order:
            b = str(bu).strip()
            v = float(bu_bill_agg.get((b, bd), 0.0))
            line_total += v
            if b.startswith("APAC-"):
                apac_sum += v
        row: dict = {"分行维度": bd}
        for label in BY_BU_DROPLIST:
            if label in ("bu", "合计"):
                row[label] = line_total
            elif label == "APAC":
                row[label] = apac_sum
            else:
                row[label] = float(
                    bu_bill_agg.get((str(label).strip(), bd), 0.0)
                )
        rows.append(row)
    df = pd.DataFrame(rows, columns=["分行维度", *list(BY_BU_DROPLIST)])
    tot: dict = {"分行维度": "合计"}
    for lab in BY_BU_DROPLIST:
        tot[lab] = float(pd.to_numeric(df[lab], errors="coerce").fillna(0.0).sum())
    df = pd.concat([df, pd.DataFrame([tot])], ignore_index=True)
    return df


# 独立隐藏表、纯 ASCII 名，作 C1「序列」来源（=CfgList!$A$1:$A$n），避免同表列引用在部分环境下无下拉
_DV_CFG_SHEET = "CfgList"


def _apply_list_validation_c1(
    ws,
    *,
    droplist: tuple[str, ...],
    default_value: str,
) -> None:
    from openpyxl.worksheet.datavalidation import DataValidation

    wb = ws.parent
    if _DV_CFG_SHEET in wb.sheetnames:
        wb.remove(wb[_DV_CFG_SHEET])
    wv = wb.create_sheet(_DV_CFG_SHEET, -1)
    wv.sheet_state = "hidden"
    n = len(droplist)
    for i, t in enumerate(droplist, start=1):
        wv.cell(row=i, column=1, value=t)
    f1 = f"={_DV_CFG_SHEET}!$A$1:$A${n}"
    dv = DataValidation(
        type="list",
        formula1=f1,
        showDropDown=False,
        allow_blank=True,
    )
    dv.add("C1")
    ws.add_data_validation(dv)
    ws["C1"] = default_value


def _xlf_q_cost_sheet(n: str) -> str:
    s = n.replace("'", "''")
    return f"'{s}'!"


def _cost_grand_sumifs(
    *,
    cost_q: str,
    bu_first: str,
    bu_last: str,
    c_br: str,
    c_mo: str,
    rowr: int,
    mo_h: str,
) -> str:
    return (
        f"SUMIFS(INDEX({cost_q}{bu_first}2:{bu_last}5000,0,"
        f"MATCH($C$1,{cost_q}{bu_first}1:{bu_last}1,0)),"
        f"{cost_q}{c_br}2:{c_br}5000,$A{rowr},"
        f"{cost_q}{c_mo}2:{c_mo}5000,{mo_h}$2)"
    )


def _cost_total_sumifs(
    *, cost_q: str, c_col: str, c_br: str, c_mo: str, rowr: int, mo_h: str
) -> str:
    return (
        f"SUMIFS({cost_q}{c_col}2:{c_col}5000,{cost_q}{c_br}2:{c_br}5000,$A{rowr},"
        f"{cost_q}{c_mo}2:{c_mo}5000,{mo_h}$2)"
    )


# 与校验版 by BU 汇总表「月份×渠道」区列宽同量级（B=18.27 等，曾采自 校验 渠道 页作参照）
_BU_BILL_SHEET_WIDTH_REF_校验: dict[str, float] = {
    "A": 26.0,
    "B": 18.27,
    "C": 16.0,
    "D": 15.18,
    "rest": 13.63,
}


def _apply_by_bu_bill_dim_col_widths(ws, *, n_droplists: int) -> None:
    """上表+全量区：A 分行维度名加宽，B–D 为两月+合计，第 5 列起业务线列用略窄默认（与原校验多列同量级）。"""
    from openpyxl.utils import get_column_letter

    ref = _BU_BILL_SHEET_WIDTH_REF_校验
    d = ws.column_dimensions
    d["A"].width = ref["A"]
    d["B"].width = ref["B"]
    d["C"].width = ref["C"]
    d["D"].width = ref["D"]
    w_rest = ref["rest"]
    for cidx in range(5, 2 + n_droplists):
        d[get_column_letter(cidx)].width = w_rest


def _cost_apac_plus(
    *,
    cost_q: str,
    apac_bus: list[str],
    bu_letters: dict[str, str],
    c_br: str,
    c_mo: str,
    rowr: int,
    mo_h: str,
) -> str:
    if not apac_bus:
        return "0"
    parts: list[str] = []
    for b in apac_bus:
        lt = bu_letters.get(str(b).strip())
        if not lt:
            continue
        parts.append(
            f"SUMIFS({cost_q}{lt}2:{lt}5000,{cost_q}{c_br}2:{c_br}5000,$A{rowr},"
            f"{cost_q}{c_mo}2:{c_mo}5000,{mo_h}$2)"
        )
    if not parts:
        return "0"
    return "+".join(parts)


def write_by_bu_bill_dim_sheet(
    writer: pd.ExcelWriter,
    df: pd.DataFrame,
    droplist: tuple[str, ...],
    *,
    sheet_name: str,
    bu_sheet_cols: list[str],
    cost_sheet_name: str = SHEET_MERGED_FIVE,
    period: int = PERIOD,
) -> None:
    """
    C1：业务线；上表=SUMIFS(INDEX(成本明细!BU 区,0,MATCH($C$1,表头)))×分行+月；全量区接在空行后。
    """
    from openpyxl.styles import Font
    from openpyxl.utils import get_column_letter

    wb = writer.book
    name = sheet_name[:31]
    if name in wb.sheetnames:
        del wb[name]
    ws = wb.create_sheet(name)
    p = int(period)
    mo = p % 100
    yr = p // 100
    if mo >= 2:
        m1, m2 = yr * 100 + mo - 1, p
    else:
        m1, m2 = (yr - 1) * 100 + 12, p

    default_c1 = "主站" if "主站" in droplist else (droplist[0] if droplist else "")
    nrows_body = len(df)
    ncols = len(droplist)
    if ncols < 1 or nrows_body < 1:
        return
    n_bu = len(bu_sheet_cols)
    c_br = get_column_letter(3)  # 成本明细 列3=渠道名称（分行维度）
    c_mo = get_column_letter(6)  # month
    j0 = 10  # 成本明细 前 9 列元数据，BU 自第 10 列起
    bfirst = get_column_letter(j0)
    last_bu = get_column_letter(j0 + n_bu - 1)
    ctot = get_column_letter(j0 + n_bu)
    cost_n = (cost_sheet_name or SHEET_MERGED_FIVE)[:31]
    cost_q = _xlf_q_cost_sheet(cost_n)
    bu_letters: dict[str, str] = {
        str(b).strip(): get_column_letter(j0 + k) for k, b in enumerate(bu_sheet_cols)
    }
    apac_bu = [b for b in bu_sheet_cols if str(b).strip().startswith("APAC-")]

    r_head = 2
    r_data0 = 3
    ws["A1"] = "BU"
    ws["B1"] = "选择业务线"
    hfont = Font(bold=True)
    ws.cell(row=r_head, column=1, value=COL_BILL_BR).font = hfont
    ws.cell(row=r_head, column=2, value=int(m1)).font = hfont
    ws.cell(row=r_head, column=3, value=int(m2)).font = hfont
    ws.cell(row=r_head, column=4, value="合计").font = hfont
    mo_b, mo_c = "B", "C"

    for i in range(nrows_body):
        r = r_data0 + i
        a0 = df.iat[i, 0]
        ws.cell(row=r, column=1, value=None if a0 is not None and pd.isna(a0) else a0)
        lab = (str(a0).strip() if a0 is not None and not (isinstance(a0, float) and pd.isna(a0)) else "")
        is_heji = lab == "合计" or str(lab) == "合计"
        if is_heji:
            prev = r - 1
            if r > r_data0:
                ws.cell(row=r, column=2, value=f"=SUM(B{r_data0}:B{prev})")
                ws.cell(row=r, column=3, value=f"=SUM(C{r_data0}:C{prev})")
                ws.cell(row=r, column=4, value=f"=SUM(D{r_data0}:D{prev})")
            else:
                ws.cell(row=r, column=2, value=0.0)
                ws.cell(row=r, column=3, value=0.0)
                ws.cell(row=r, column=4, value=0.0)
            continue
        g_b = _cost_grand_sumifs(
            cost_q=cost_q,
            bu_first=bfirst,
            bu_last=last_bu,
            c_br=c_br,
            c_mo=c_mo,
            rowr=r,
            mo_h=mo_b,
        )
        g_c = _cost_grand_sumifs(
            cost_q=cost_q,
            bu_first=bfirst,
            bu_last=last_bu,
            c_br=c_br,
            c_mo=c_mo,
            rowr=r,
            mo_h=mo_c,
        )
        t_b = _cost_total_sumifs(
            cost_q=cost_q, c_col=ctot, c_br=c_br, c_mo=c_mo, rowr=r, mo_h=mo_b
        )
        t_c = _cost_total_sumifs(
            cost_q=cost_q, c_col=ctot, c_br=c_br, c_mo=c_mo, rowr=r, mo_h=mo_c
        )
        ap_b = _cost_apac_plus(
            cost_q=cost_q,
            apac_bus=apac_bu,
            bu_letters=bu_letters,
            c_br=c_br,
            c_mo=c_mo,
            rowr=r,
            mo_h=mo_b,
        )
        ap_c = _cost_apac_plus(
            cost_q=cost_q,
            apac_bus=apac_bu,
            bu_letters=bu_letters,
            c_br=c_br,
            c_mo=c_mo,
            rowr=r,
            mo_h=mo_c,
        )
        inner_b = f'=IF($C$1="合计",{t_b},IF($C$1="APAC",{ap_b},IF($C$1="bu",{t_b},IFERROR({g_b},0))))'
        inner_c = f'=IF($C$1="合计",{t_c},IF($C$1="APAC",{ap_c},IF($C$1="bu",{t_c},IFERROR({g_c},0))))'
        ws.cell(row=r, column=2, value=inner_b)
        ws.cell(row=r, column=3, value=inner_c)
        ws.cell(row=r, column=4, value=f"=SUM(B{r}:C{r})")

    # 与「收付款成本分摊-2026.02校验版.xlsx」中「by BU by 渠道」一致：仅单块「渠道名称（分行维度）× 月列」联动表，不另起「全量」矩阵
    ws.freeze_panes = f"B{r_data0}"
    _apply_list_validation_c1(ws, droplist=droplist, default_value=default_c1)
    # 上表仅占用 A–D 列，不对 E 以后按「全量多业务线列」设宽
    _apply_by_bu_bill_dim_col_widths(ws, n_droplists=2)


def main() -> None:
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(TEMPLATE_PATH)
    if not SUMMARY_PATH.exists():
        raise FileNotFoundError(SUMMARY_PATH)

    _ensure_bu_cols_loaded()
    mmap = load_mapping()
    fin, fout, va_raw, global_tot = load_bases(PERIOD)
    va = _standardize_va_base(va_raw, PERIOD)

    sm = pd.read_excel(SUMMARY_PATH, sheet_name=SUMMARY_SHEET, engine="calamine")
    need_cols = {"渠道名称（分行维度）", "主体", "month"} | {c for c, _ in SUMMARY_BUCKET_COLS}
    miss_hdr = need_cols - set(sm.columns.astype(str))
    if miss_hdr:
        raise RuntimeError(f"汇总表缺少列: {sorted(miss_hdr)}")
    per_sm = pd.to_numeric(sm["month"], errors="coerce")
    m_ok = (per_sm == PERIOD) | sm["month"].astype(str).str.contains(str(PERIOD), na=False)
    sm = sm.loc[m_ok].copy()
    sm = _prepare_sm_for_allocation(sm)
    citihk_pphk_listed_accts = _citihk_pphk_listed_account_keys_from_summary(sm)

    out_in: list[dict] = []
    out_ou: list[dict] = []
    out_ot: list[dict] = []
    out_rcv: list[dict] = []
    out_va: list[dict] = []

    unmapped_bill_cfs: set[str] = set()
    bu_sheet_cols_base: list[str] = []
    seen_bu: set[str] = set()
    for c in BU_COLS_RAW:
        t = str(c).strip() if c is not None and not (isinstance(c, float) and pd.isna(c)) else ""
        if not t or t.lower() == "nan":
            continue
        if t not in seen_bu:
            bu_sheet_cols_base.append(t)
            seen_bu.add(t)
    _ensure_required_bu_tail(bu_sheet_cols_base, seen_bu)

    template_bu_names = _template_bu_name_set()
    extra_bu_list: list[str] = []
    extra_bu_seen: set[str] = set()
    pending: list[dict] = []
    special_hits: defaultdict[str, int] = defaultdict(int)

    for _, row in sm.iterrows():
        bill_cf = _norm_bill_cf(row["渠道名称（分行维度）"])
        if not bill_cf:
            continue
        ent_cf = _norm_entity_cf(row["主体"])
        acct_k = _norm_account_key(row["账号"]) if "账号" in sm.columns else None
        detail_acct = acct_k
        is_citi = bill_cf == "citihk"
        acct_token = acct_k if is_citi else "__all__"
        ent_show = _cell_text(row["主体"])

        for sum_col, bucket in SUMMARY_BUCKET_COLS:
            _v = pd.to_numeric(row.get(sum_col), errors="coerce")
            amt = 0.0 if pd.isna(_v) else float(_v)
            if amt == 0.0:
                continue
            rec = mmap.get(bill_cf)
            if not rec:
                if bill_cf:
                    unmapped_bill_cfs.add(bill_cf)
                rec = _default_mapping_rec(bill_cf, _cell_text(row["渠道名称（分行维度）"]))

            bill_name = rec["bill"]
            corr_nf = _norm_branch(rec["corr"])
            account_key: str | None = None
            if is_citi:
                account_key = None if acct_token == "__all__" or acct_token is None else str(acct_token)

            weight_account_key = account_key
            if (
                is_citi
                and ent_cf == "pphk"
                and bucket in ("inbound", "outbound", "others")
                and account_key == CITIHK_OUTBOUND_AGGREGATE_ACCOUNT
            ):
                weight_account_key = None

            code = resolve_method(bucket, rec)
            if is_citi and ent_cf == "pphk" and bucket == "VA":
                weight_account_key = None
                code = "tot_cnt"
            bill_display_row = _cell_text(row.get("渠道名称（分行维度）", ""))
            sp = pre_special_alloc_weights(
                bill_cf=bill_cf,
                bill_display=bill_display_row,
                ent_cf=ent_cf,
                bucket=bucket,
                account_key=detail_acct,
                rec=rec,
                code=code,
                fin=fin,
                fout=fout,
                va=va,
                global_tot=global_tot,
                month=PERIOD,
            )
            if sp is not None:
                w_series, how, rule_id = sp
                special_hits[rule_id] += 1
            elif (
                is_citi
                and ent_cf == "pphk"
                and account_key is None
                and bucket in ("inbound", "outbound", "others")
                and code != "fixed_global"
            ):
                w_series, how = weights_citihk_pphk_no_acct_residual(
                    code,
                    fin=fin,
                    fout=fout,
                    va=va,
                    global_tot=global_tot,
                    month=PERIOD,
                    entity_cf=ent_cf,
                    corr_nf=corr_nf,
                    listed_accounts=citihk_pphk_listed_accts,
                )
            else:
                w_series, how = weights_for(
                    code,
                    fin=fin,
                    fout=fout,
                    va=va,
                    global_tot=global_tot,
                    month=PERIOD,
                    entity_cf=ent_cf,
                    corr_nf=corr_nf,
                    account_key=weight_account_key,
                )
                if not how:
                    how = method_label(code, rec, bucket)

            _register_extra_bus_from_weights(
                w_series,
                template_names=template_bu_names,
                extra_list=extra_bu_list,
                extra_seen=extra_bu_seen,
            )
            is_beepay = ent_cf == "pphk" and re.search("beepay", bill_display_row + " " + bill_cf, re.I) is not None
            pending.append(
                {
                    "amt": amt,
                    "w_series": w_series,
                    "how": how,
                    "bucket": bucket,
                    "bill_name": bill_name,
                    "corr": rec["corr"],
                    "ent_show": ent_show,
                    "ent_cf": ent_cf,
                    "bill_cf": bill_cf,
                    "account_key": account_key,
                    "bill_display": bill_display_row,
                    "beepay": is_beepay,
                }
            )

    bu_unique: list[str] = []
    seen_u2: set[str] = set()
    for c in bu_sheet_cols_base:
        if c not in seen_u2:
            bu_unique.append(c)
            seen_u2.add(c)

    full_bu_keys = bu_unique + extra_bu_list
    bu_sheet_cols = bu_sheet_cols_base + extra_bu_list

    all_detail_row_lists: list[list[dict]] = [
        out_in,
        out_ou,
        out_ot,
        out_rcv,
        out_va,
    ]

    for p in pending:
        amt = p["amt"]
        w_series = p["w_series"]
        how = p["how"]
        bucket = p["bucket"]
        bill_name = p["bill_name"]
        ent_show = p["ent_show"]
        account_key = p["account_key"]
        use_pphk_sheet = _is_citi_hk_pphk_sheet_row(
            bill_cf=p["bill_cf"], corr_display=p["corr"], ent_cf=p["ent_cf"]
        )

        dist_u = distribute(amt, w_series, full_bu_keys)
        if p.get("beepay"):
            dist_u = apply_beepay_smb8_to_smb_from_partnership(dist_u, cost=amt)
            special_hits["SR_BEEPAY_8PCT"] += 1
        bu_vec = _bu_values_extended(dist_u, extra_bu_list, col_order=bu_sheet_cols_base)

        r = {
            COL_BILL: bill_name,
            COL_BILL_BR: _cell_text(p.get("bill_display", "")),
            COL_CHANNEL: p["corr"],
            COL_ENTITY: ent_show,
            COL_MONTH: PERIOD,
        }
        if bucket in ("inbound", "outbound", "others", "收款通道成本"):
            r[COL_ACCOUNT] = (account_key or "") if use_pphk_sheet else ""
        if bucket == "inbound":
            r["inbound成本"] = amt
        elif bucket == "outbound":
            r["outbound成本"] = amt
        elif bucket == "others":
            r["others 成本"] = amt
        elif bucket == "收款通道成本":
            r["收款通道成本"] = amt
        elif bucket == "VA":
            r["VA成本"] = amt
        r[COL_ALLOC] = how
        r["_bu"] = bu_vec
        r[COL_BILL_DIM] = _cell_text(p.get("bill_display", ""))

        if bucket == "inbound":
            out_in.append(r)
        elif bucket == "outbound":
            out_ou.append(r)
        elif bucket == "others":
            out_ot.append(r)
        elif bucket == "收款通道成本":
            out_rcv.append(r)
        elif bucket == "VA":
            out_va.append(r)

    G_zhengti = _sum_overall_from_detail_rows(all_detail_row_lists, bu_sheet_cols)
    full_bu_keys_for_zt: list[str] = list(full_bu_keys)
    if "收单" not in full_bu_keys_for_zt:
        full_bu_keys_for_zt.append("收单")
    zt_second = compute_zhengti_second_split(
        G_zhengti,
        full_bu_keys=full_bu_keys_for_zt,
        global_tot=global_tot,
        fx_fixed=ZHENGTI_RESERVE_FX,
        shoudan_fixed=ZHENGTI_RESERVE_SHOUDAN,
    )

    bu_bill_agg: defaultdict[tuple[str, str], float] = defaultdict(float)
    for rows in all_detail_row_lists:
        for r in rows:
            bd_key = _cell_text(r.get(COL_BILL_DIM, ""))
            if not bd_key:
                bd_key = _cell_text(r.get(COL_BILL, ""))
            vec = r.get("_bu")
            if vec is None or len(vec) != len(bu_sheet_cols):
                continue
            for j, b in enumerate(bu_sheet_cols):
                b0 = str(b).strip()
                bu_bill_agg[(b0, bd_key)] += float(vec[j])

    if unmapped_bill_cfs:
        miss = sorted(unmapped_bill_cfs)
        open(BASE_DIR / "_allocate_unmapped_bills.json", "w", encoding="utf-8").write(
            json.dumps(miss, ensure_ascii=False, indent=2)
        )
        print("提示: 以下「分行维度」在 mapping 无键，已按汇总行对应渠道+总笔数(回退) 出摊，见 _allocate_unmapped_bills.json")

    df_in = build_sheet_rows(
        out_in, "inbound成本", with_account=True, bu_sheet_cols=bu_sheet_cols
    )
    df_ou = build_sheet_rows(
        out_ou, "outbound成本", with_account=True, bu_sheet_cols=bu_sheet_cols
    )
    df_ot = build_sheet_rows(
        out_ot, "others 成本", with_account=True, bu_sheet_cols=bu_sheet_cols
    )
    df_rcv = build_sheet_rows(
        out_rcv, "收款通道成本", with_account=True, bu_sheet_cols=bu_sheet_cols
    )
    df_va = build_sheet_rows(out_va, "VA成本", with_account=False, bu_sheet_cols=bu_sheet_cols)
    sheets = {
        SHEET_MERGED_FIVE: merge_five_cost_dataframes(
            df_in, df_ou, df_ot, df_rcv, df_va, bu_sheet_cols=bu_sheet_cols
        ),
    }

    pivot_df = build_fentan_pivot(
        sheets,
        PERIOD,
        extra_bus=extra_bu_list,
        zt_second_split=zt_second if G_zhengti > 0 else None,
    )

    df_bu_bill = build_bu_by_bill_dim_df(bu_bill_agg, bu_order=full_bu_keys)

    with pd.ExcelWriter(OUT_PATH, engine="openpyxl") as w:
        pivot_df.to_excel(w, sheet_name="分摊结果", index=False, header=False)
        for name, sdf in sheets.items():
            sdf.to_excel(w, sheet_name=name[:31], index=False)
        write_by_bu_bill_dim_sheet(
            w,
            df_bu_bill,
            BY_BU_DROPLIST,
            sheet_name=SHEET_BY_BU_BILL,
            bu_sheet_cols=bu_sheet_cols,
            cost_sheet_name=SHEET_MERGED_FIVE,
            period=PERIOD,
        )

    print("written:", OUT_PATH)
    _write_special_rule_log(special_hits, PERIOD)


def _write_special_rule_log(hits: dict[str, int], period: int) -> None:
    """汇总「分摊规则」特殊处理各条命中次数，并输出未命中项到控制台与 json。"""
    full: dict[str, int] = {rid: int(hits.get(rid, 0)) for rid, _ in SPECIAL_RULE_CATALOG}
    for k, v in hits.items():
        if k not in full and v:
            full[k] = v
    not_effective: list[dict[str, str]] = [
        {"id": rid, "desc": desc} for rid, desc in SPECIAL_RULE_CATALOG if full.get(rid, 0) == 0
    ]
    payload = {
        "period": period,
        "hits": full,
        "not_effective": not_effective,
        "note": "not_effective 表示本期「模板口径汇总」中没有任何成本桶行触发该条；Beepay 在第二段按有 Beepay 标志的桶计数。无触发多因无对应渠道/主体或起量后走常规模型。",
    }
    SPECIAL_RULE_LOG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    pos = {k: v for k, v in full.items() if v > 0}
    if pos:
        print("特殊规则-本期有命中:", pos)
    else:
        print("特殊规则-本期有命中: (无)")
    if not_effective:
        print("特殊规则-本期未命中 (若应有业务请检查汇总列与条件):")
        for x in not_effective:
            print(f"  - {x['id']}: {x['desc']}")
    else:
        print("特殊规则-本期未命中: (无，全部至少命中一次)")
    print("特殊规则日志文件:", SPECIAL_RULE_LOG_PATH)


if __name__ == "__main__":
    main()
