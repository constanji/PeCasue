"""与执行匹配页一致的规则校验逻辑，供期次与来源等根据落盘 xlsx 汇总状态。"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .constants import OUTPUT_COLUMNS
from .match import _normalize_db_region_tag

# 与 own_flow.rules_from_xlsx._COL_MAP 一致；勿把 Name/Address 映射到 Payment Details（会误判 Queen Bee）
_COL_MAP: dict[str, str] = {
    "Transaction Description": "Transaction Description",
    "Payment Details": "Payment Details",
    "Name/Address": "Name/Address",
    "Description": "Transaction Description",
    "mark（财务）": "mark（财务）",
}
_WILDCARD_ENTITIES = frozenset({"各主体", "其他主体", "*", ""})
_WILDCARD_CHANNELS = frozenset({"各主体", "其他主体", "*", ""})


def _channel_is_specific(channel: str) -> bool:
    return str(channel or "").strip() not in _WILDCARD_CHANNELS


def _entity_is_specific(entity: str) -> bool:
    return entity not in _WILDCARD_ENTITIES


def _parse_amount_ceiling(processing: str) -> float | None:
    m = re.search(r"[（(]金额[低小]于(\d+(?:\.\d+)?)[）)]", processing or "")
    if m:
        return float(m.group(1))
    m = re.search(r"[（(].*?[≤<=](\d+(?:\.\d+)?)[）)]", processing or "")
    if m:
        return float(m.group(1))
    return None


def parse_rules_from_dataframe(df: pd.DataFrame) -> list[dict]:
    """从处理表 DataFrame 解析可机读规则行（与流水线 ``rules_from_xlsx`` 空表头兜底一致）。"""
    if df.empty:
        return []
    df = df.copy()
    if "数据源" in df.columns:
        df = df[df["数据源"].astype(str).str.strip() == "自有流水"].copy()

    rules: list[dict] = []
    for pos, (_, row) in enumerate(df.iterrows()):
        col_header = str(row.get("表头", "") or "").strip()
        processing = str(row.get("处理", "") or "").strip()
        remark = str(row.get("备注", "") or "").strip()
        acct_subj = str(row.get("入账科目", "") or "").strip()
        channel = str(row.get("渠道", "") or "").strip()
        entity = str(row.get("主体", "") or "").strip()
        file_tag = str(row.get("文件", "") or "").strip()
        note = str(row.get("说明", "") or "").strip()

        if remark.lower() == "nan":
            remark = ""
        if acct_subj.lower() == "nan":
            acct_subj = ""
        if note.lower() == "nan":
            note = ""
        if not processing or processing.lower() == "nan":
            continue
        if "需人为" in processing or "人为判断" in processing:
            continue
        _pcompact = processing.replace(" ", "")
        if "那条" in processing or "金额100多" in _pcompact:
            continue

        if (not col_header or col_header.lower() == "nan") and channel == "BOSH" and "手续费" in processing:
            col_header = "Transaction Description"
        ft_compact = file_tag.lower().replace(" ", "")
        ch_up = channel.upper().strip()
        if (not col_header or col_header.lower() == "nan") and (
            ch_up == "RUMG"
            or "rumg" in ft_compact
            or ch_up == "MUFG"
            or "mufg" in ft_compact
        ):
            col_header = "Transaction Description"

        if not col_header or col_header.lower() == "nan":
            continue

        excel_row_num = pos + 2
        rules.append({
            "row": excel_row_num,
            "渠道": channel if channel.lower() != "nan" else "",
            "主体": entity if entity.lower() != "nan" else "",
            "文件": file_tag if file_tag.lower() != "nan" else "",
            "表头_原始": col_header,
            "表头_输出": _COL_MAP.get(col_header, col_header),
            "处理": processing,
            "期望备注": remark,
            "期望入账科目": acct_subj,
            "说明": note,
        })
    return rules


def parse_rules_from_xlsx(path: Path) -> list[dict]:
    path = path.expanduser().resolve()
    stem = path.parent / path.stem
    csv_p = stem.with_suffix(".csv")
    if csv_p.is_file():
        df = pd.read_csv(csv_p, encoding="utf-8-sig")
    elif path.is_file():
        df = pd.read_excel(path, engine="openpyxl")
    else:
        return []
    return parse_rules_from_dataframe(df)


def parse_rules_from_json(path: Path) -> list[dict]:
    """解析 ``own_flow_processing/current.json``。"""
    path = path.expanduser().resolve()
    if not path.is_file():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    rows = raw.get("rows") or []
    if not rows:
        return []
    return parse_rules_from_dataframe(pd.DataFrame(rows))


def _parse_match_condition(processing: str) -> tuple[str, str]:
    """与 own_flow.rules_from_xlsx._parse_processing_to_kind_pattern 对齐，避免验证与流水线语义不一致。"""
    from .rules_from_xlsx import _parse_processing_to_kind_pattern

    try:
        kind, pat = _parse_processing_to_kind_pattern(processing)
    except ValueError:
        s = (processing or "").strip()
        return "contains", s
    if kind == "icontains":
        return "contains", pat
    if kind == "istartswith":
        return "startswith", pat
    if kind == "iexact":
        return "exact", pat
    if kind == "iregex":
        return "regex", pat
    return "contains", pat


def _source_file_matches(file_tag: str, channel: str, source_file: str) -> bool:
    """与 verify_rules._source_file_matches 一致，避免 PPUS-USD / DB 三分行等误报「无匹配来源」。"""
    sf = source_file.lower()
    ft = file_tag.lower().replace("nan", "")
    ch = channel.lower()

    if ft in ("pphk", "pphk主文件"):
        return "pphk" in sf and "other" not in sf and sf.endswith(".csv")
    if "pphk" in ft and "other" in ft:
        return "pphk" in sf and "other" in sf and sf.endswith(".csv")
    if ft in ("其他文件",):
        # 排除 PPHK 主/other 已由专用「文件」行覆盖；亦排除 PPUS-USD（有独立规则），否则会误把 citi_other 行当「其他文件」用 billing 去对账（如 BDD 小额行实际走 iexact+阈值规则）
        if "pphk" in sf:
            return False
        if "ppus" in sf and "usd" in sf.replace(" ", ""):
            return False
        return sf.endswith(".csv")
    if "ppus" in ft and "usd" in ft:
        return "ppus" in sf and "usd" in sf and sf.endswith(".csv")
    if "ppeu" in ft:
        return "ppeu" in sf and sf.endswith(".xlsx")
    if "jpm" in ft:
        return "jpm" in sf
    if ft == "boc":
        return "boc" in sf
    if ft == "db":
        return "db" in sf and "自有流水" in sf
    if ft.replace("-", "") in ("dbhk", "dbkr", "dbth") or ft in ("db-hk", "db-kr", "db-th"):
        return ch == "db" and sf.endswith(".csv") and "online_rpt" in sf
    if "dbs" in ft and "流水" in ft:
        return "dbs" in sf and (sf.endswith(".xls") or sf.endswith(".xlsx"))

    if ch == "scb":
        return "scb" in sf
    if ch == "bosh":
        return "bosh" in sf

    ft_compact = ft.replace(" ", "")
    ch_l = (channel or "").strip().lower()
    if ch_l in ("rumg", "mufg") or "rumg" in ft_compact or "mufg" in ft_compact:
        return sf.endswith(".csv") and ("rumg" in sf or "mufg" in sf)
    return False


def _cell_str(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def _full_row_after_match(s: pd.Series) -> dict[str, str]:
    """与「执行匹配标注」导出列一致，便于对照流水线结果。"""
    out: dict[str, str] = {}
    for col in OUTPUT_COLUMNS:
        if col not in s.index:
            out[col] = ""
            continue
        v = s[col]
        if v is None or (isinstance(v, float) and pd.isna(v)):
            out[col] = ""
        else:
            out[col] = str(v)
    return out


def _row_should_match(row, col_name: str, match_type: str, pattern: str) -> bool:
    val = _cell_str(row.get(col_name, ""))
    if not val:
        return False
    if match_type == "contains":
        return pattern.lower() in val.lower()
    if match_type == "startswith":
        vl = val.lower()
        pl = pattern.lower()
        if vl.startswith(pl):
            return True
        if pl.startswith("ach-") and vl.startswith("ach") and not vl.startswith("ach-"):
            return True
        return False
    if match_type == "exact":
        return val.lower() == pattern.lower()
    if match_type == "regex":
        return re.search(pattern, val, re.IGNORECASE) is not None
    return False


def verify_structured(rules: list[dict], df: pd.DataFrame) -> list[dict]:
    results: list[dict] = []
    for idx, rule in enumerate(rules):
        match_type, pattern = _parse_match_condition(rule["处理"])
        out_col = rule["表头_输出"]
        expected_remark = rule["期望备注"]
        expected_subj = rule["期望入账科目"]
        note = str(rule.get("说明", "") or "")

        file_candidates = df[df["来源文件"].apply(
            lambda sf, _ft=rule["文件"], _ch=rule["渠道"]: _source_file_matches(_ft, _ch, sf)
        )]

        if len(file_candidates) == 0:
            results.append({
                "规则序号": idx + 1, "渠道": rule["渠道"], "主体": rule["主体"],
                "文件": rule["文件"],
                "条件": f"{rule['表头_原始']} {match_type} '{pattern}'",
                "期望备注": expected_remark or "(不限)",
                "期望入账科目": expected_subj or "(不限)",
                "状态": "不适用",
                "说明": "本期汇总明细中未包含该规则对应的来源文件，跳过核对",
                "命中行数": 0, "不一致数": 0, "问题行": [],
            })
            continue

        candidates = file_candidates

        rule_entity = rule["主体"]
        if _entity_is_specific(rule_entity) and "主体" in df.columns:
            ent_u = rule_entity.strip().upper()
            subj = candidates["主体"].fillna("").str.strip().str.upper()
            if (
                ent_u == "PPUS"
                and "PPHK" in note
                and "EFT" in str(rule.get("处理", "")).upper()
            ):
                candidates = candidates[subj.isin({"PPUS", "PPHK"})]
            else:
                candidates = candidates[subj == ent_u]

        rule_ch = rule["渠道"]
        if "渠道" in df.columns and _channel_is_specific(rule_ch) and len(candidates) > 0:
            candidates = candidates[
                candidates["渠道"].fillna("").str.strip() == str(rule_ch).strip()
            ]

        ftag_norm = _normalize_db_region_tag(rule["文件"])
        if (
            rule["渠道"].strip().upper() == "DB"
            and ftag_norm in ("DB-HK", "DB-KR", "DB-TH")
            and "分行维度" in df.columns
            and len(candidates) > 0
        ):
            br = candidates["分行维度"].fillna("").astype(str).str.strip()
            candidates = candidates[br.apply(lambda x, fn=ftag_norm: _normalize_db_region_tag(x) == fn)]

        if len(candidates) == 0:
            results.append({
                "规则序号": idx + 1, "渠道": rule["渠道"], "主体": rule["主体"],
                "文件": rule["文件"],
                "条件": f"{rule['表头_原始']} {match_type} '{pattern}'",
                "期望备注": expected_remark or "(不限)",
                "期望入账科目": expected_subj or "(不限)",
                "状态": "警告",
                "说明": "来源文件已匹配，但主体/渠道/分行筛选后无明细行（请核对规则与汇总数据）",
                "命中行数": 0, "不一致数": 0, "问题行": [],
            })
            continue

        if out_col not in df.columns:
            results.append({
                "规则序号": idx + 1, "渠道": rule["渠道"], "主体": rule["主体"],
                "文件": rule["文件"],
                "条件": f"{rule['表头_原始']} → '{out_col}' 列不存在",
                "期望备注": expected_remark or "(不限)",
                "期望入账科目": expected_subj or "(不限)",
                "状态": "警告", "说明": f"输出列 '{out_col}' 不存在",
                "命中行数": 0, "不一致数": 0, "问题行": [],
            })
            continue

        is_db_charge = (
            rule["渠道"].strip().upper() == "DB"
            and ftag_norm in ("DB-KR", "DB-TH")
            and match_type == "regex"
            and pattern.strip().lower() == r"^charge$"
        )
        if is_db_charge:
            should_match = candidates
        else:
            should_match = candidates[
                candidates.apply(
                    lambda r, _c=out_col, _mt=match_type, _p=pattern: _row_should_match(
                        r, _c, _mt, _p
                    ),
                    axis=1,
                )
            ]

        amt_ceil = _parse_amount_ceiling(rule["处理"])
        if amt_ceil is not None and "Transaction Amount" in should_match.columns:
            should_match = should_match[
                pd.to_numeric(should_match["Transaction Amount"], errors="coerce").abs()
                <= amt_ceil
            ]

        if len(should_match) == 0:
            results.append({
                "规则序号": idx + 1, "渠道": rule["渠道"], "主体": rule["主体"],
                "文件": rule["文件"],
                "条件": f"{rule['表头_原始']} {match_type} '{pattern}'",
                "期望备注": expected_remark or "(不限)",
                "期望入账科目": expected_subj or "(不限)",
                "状态": "警告", "说明": f"来源 {len(candidates)} 行，0 行满足条件",
                "命中行数": 0, "不一致数": 0, "问题行": [],
            })
            continue

        mismatched_rows = []
        for row_idx, r in should_match.iterrows():
            actual_remark = _cell_str(r.get("备注"))
            actual_subj = _cell_str(r.get("入账科目"))
            remark_ok = (not expected_remark) or (actual_remark == expected_remark)
            subj_ok = (not expected_subj) or (actual_subj == expected_subj)
            if not remark_ok or not subj_ok:
                mismatched_rows.append({
                    "行号": row_idx,
                    "实际备注": actual_remark or "(空)",
                    "期望备注": expected_remark or "(不限)",
                    "实际入账科目": actual_subj or "(空)",
                    "期望入账科目": expected_subj or "(不限)",
                    "Transaction Description": _cell_str(r.get("Transaction Description"))[:60],
                    "来源文件": _cell_str(r.get("来源文件")),
                    "_full_row": _full_row_after_match(r),
                })

        status = "通过" if len(mismatched_rows) == 0 else "待核算"
        results.append({
            "规则序号": idx + 1, "渠道": rule["渠道"], "主体": rule["主体"],
            "文件": rule["文件"],
            "条件": f"{rule['表头_原始']} {match_type} '{pattern}'",
            "期望备注": expected_remark or "(不限)",
            "期望入账科目": expected_subj or "(不限)",
            "状态": status,
            "说明": f"命中 {len(should_match)} 行，不一致 {len(mismatched_rows)} 行" if mismatched_rows else f"命中 {len(should_match)} 行，全部正确",
            "命中行数": len(should_match), "不一致数": len(mismatched_rows),
            "问题行": mismatched_rows,
        })
    return results


def default_rules_file() -> Path:
    from server.core.paths import get_rules_files_dir

    return get_rules_files_dir() / "rules" / "处理表.xlsx"


# v2：修正「仅落盘缺 mark（财务）列却按全表校验」导致的错误摘要；旧 v1 侧车一律重算
VERIFY_SUMMARY_SCHEMA = "allline_ownflow_verify_v2"
VERIFY_SUMMARY_FILENAME = "verify_summary.json"

# 与 pages/6_执行匹配._df_for_user_export 一致：导出 xlsx 不含此列；对落盘文件校验前补空列，避免误报「列不存在」
_EXPORT_OMITTED_FIN_MARK = "mark（财务）"


def counts_from_verify_results(results: list[dict]) -> dict[str, int]:
    return {
        "total": len(results),
        "pass": sum(1 for r in results if r["状态"] == "通过"),
        "warn": sum(1 for r in results if r["状态"] == "警告"),
        "pending": sum(1 for r in results if r["状态"] == "待核算"),
        "na": sum(1 for r in results if r["状态"] == "不适用"),
    }


def compact_processing_verify_for_api(results: list[dict]) -> list[dict]:
    """去掉嵌套大对象，供 API/前端验证报告展示。"""
    out: list[dict] = []
    for r in results:
        row = {k: v for k, v in r.items() if k != "问题行"}
        issues = r.get("问题行") or []
        row["问题行总数"] = len(issues)
        row["问题行预览"] = [
            {
                "行号": x.get("行号"),
                "来源文件": x.get("来源文件"),
                "实际备注": x.get("实际备注"),
                "期望备注": x.get("期望备注"),
                "实际入账科目": x.get("实际入账科目"),
                "期望入账科目": x.get("期望入账科目"),
            }
            for x in issues[:20]
        ]
        out.append(row)
    return out


def write_verify_summary(run_dir: Path, results: list[dict]) -> Path:
    """与验证报告同一套 results 写入 run 目录，供期次与来源快速展示且数字一致。"""
    d = run_dir.expanduser().resolve()
    d.mkdir(parents=True, exist_ok=True)
    c = counts_from_verify_results(results)
    body = {
        "schema": VERIFY_SUMMARY_SCHEMA,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        **c,
    }
    p = d / VERIFY_SUMMARY_FILENAME
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2), encoding="utf-8")
    return p


def read_verify_summary(matched_xlsx: Path) -> dict[str, int] | None:
    """读取 verify_summary.json；若不存在或 xlsx 比摘要新则返回 None（需重算或视为无摘要）。"""
    x = matched_xlsx.expanduser().resolve()
    if not x.is_file():
        return None
    side = x.parent / VERIFY_SUMMARY_FILENAME
    if not side.is_file():
        return None
    try:
        if x.stat().st_mtime > side.stat().st_mtime + 1e-3:
            return None
        raw = json.loads(side.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError, KeyError):
        return None
    if raw.get("schema") != VERIFY_SUMMARY_SCHEMA:
        return None
    try:
        return {
            "total": int(raw["total"]),
            "pass": int(raw["pass"]),
            "warn": int(raw["warn"]),
            "pending": int(raw["pending"]),
            "na": int(raw.get("na", 0)),
        }
    except (KeyError, TypeError, ValueError):
        return None


def summarize_matched_excel(
    xlsx_path: Path,
    *,
    rules_file: Path | None = None,
) -> dict[str, int] | None:
    """读取 own_bank_statement_matched.xlsx，按处理表统计 通过/警告/待核算 条数；失败返回 None。

    优先使用同目录 ``verify_summary.json``（与执行匹配页写入的一致），避免每次打开期次与来源都全量重算。
    """
    p = xlsx_path.expanduser().resolve()
    if not p.is_file():
        return None
    cached = read_verify_summary(p)
    if cached is not None:
        return cached

    rf = rules_file if rules_file is not None else default_rules_file()
    try:
        df = pd.read_excel(p, engine="openpyxl")
    except (OSError, ValueError, KeyError):
        return None
    if "来源文件" not in df.columns:
        return None
    if _EXPORT_OMITTED_FIN_MARK not in df.columns:
        df = df.copy()
        df[_EXPORT_OMITTED_FIN_MARK] = ""
    rules = parse_rules_from_xlsx(rf)
    if not rules:
        return None
    try:
        results = verify_structured(rules, df)
    except Exception:
        return None
    c = counts_from_verify_results(results)
    try:
        write_verify_summary(p.parent, results)
    except OSError:
        pass
    return c
