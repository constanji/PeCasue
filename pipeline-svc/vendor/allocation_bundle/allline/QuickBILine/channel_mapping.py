"""从模板 mapping sheet 加载入金/出金渠道名称映射，将 QuickBI 原始渠道名替换为「渠道-分行」标准名。"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .quickbi_io import CHANNEL_COL

_LOG = logging.getLogger("quickbi.line")
_MAP_SHEET = "mapping"

# 表头关键字，用于自动定位入金/出金渠道列
_INBOUND_HEADER_KW = "入金渠道"
_OUTBOUND_HEADER_KW = "出金渠道"
_MAPPED_HEADER_KW = "渠道-分行"


def _find_header_row_and_cols(
    df: pd.DataFrame, raw_kw: str, mapped_kw: str
) -> tuple[int, int, int] | None:
    """在前5行中搜索含 raw_kw 和 mapped_kw 的表头，返回 (header_row, raw_col, mapped_col)。

    mapped_kw 限定在 raw_kw 所在列之后搜索（避免入金和出金共用同一个「渠道-分行」列）。
    """
    for ri in range(min(5, len(df))):
        raw_ci = None
        mapped_ci = None
        for ci in range(len(df.columns)):
            s = str(df.iloc[ri, ci]).strip()
            if raw_kw in s and raw_ci is None:
                raw_ci = ci
        if raw_ci is not None:
            for ci in range(raw_ci + 1, len(df.columns)):
                s = str(df.iloc[ri, ci]).strip()
                if mapped_kw in s:
                    mapped_ci = ci
                    break
        if raw_ci is not None and mapped_ci is not None:
            return ri, raw_ci, mapped_ci
    return None


def _normalize_citi_mapped_value(v: str) -> str:
    """CITI 相关渠道名标准化为大写（如 Citi-US → CITI-US, Cit → CITI）。"""
    upper = v.upper()
    # 只处理以 CITI 开头的值
    if upper.startswith("CITI"):
        return upper
    return v


def _load_channel_map(template: Path, raw_kw: str, mapped_kw: str) -> dict[str, str]:
    df = pd.read_excel(
        template, sheet_name=_MAP_SHEET, dtype=object,
        keep_default_na=False, na_filter=False,
    )
    result = _find_header_row_and_cols(df, raw_kw, mapped_kw)
    if result is None:
        _LOG.warning("mapping sheet 中未找到含 %r / %r 的表头行", raw_kw, mapped_kw)
        return {}
    header_ri, raw_ci, mapped_ci = result
    _LOG.info("mapping sheet: header row=%d, raw col=%d, mapped col=%d", header_ri, raw_ci, mapped_ci)
    out: dict[str, str] = {}
    for ri in range(header_ri + 1, len(df)):
        raw = str(df.iloc[ri, raw_ci]).strip()
        mapped = str(df.iloc[ri, mapped_ci]).strip()
        if raw and mapped and raw.lower() != "nan" and mapped.lower() != "nan":
            mapped = _normalize_citi_mapped_value(mapped)
            if raw not in out:
                out[raw] = mapped
    return out


def apply_channel_name_mapping(
    df: pd.DataFrame,
    *,
    template: Path,
    is_outbound: bool,
    mapping_workbook: Path | None = None,
    preserve_channel_mask: pd.Series | None = None,
) -> pd.DataFrame:
    """将 df 的「渠道名称」列通过 mapping sheet 映射为标准「渠道-分行」名。

    mapping_workbook: 若提供则从此文件读取 mapping sheet；否则从 template 读取。
    自动在 mapping sheet 中搜索含「入金渠道」/「出金渠道」和「渠道-分行」的表头，
    然后从表头下方读取映射。未命中的行保持原值。

    preserve_channel_mask: 与 df 行对齐；为 True 的行不替换渠道名（用于出金并入入金等特例）。
    """
    if df.empty or CHANNEL_COL not in df.columns:
        return df
    raw_kw = _OUTBOUND_HEADER_KW if is_outbound else _INBOUND_HEADER_KW
    src = mapping_workbook or template
    ch_map = _load_channel_map(src, raw_kw, _MAPPED_HEADER_KW)
    if not ch_map:
        _LOG.warning("渠道映射为空 (is_outbound=%s)，跳过映射", is_outbound)
        return df
    _LOG.info("渠道映射加载完成 (is_outbound=%s): %d 条映射", is_outbound, len(ch_map))
    out = df.copy()
    orig = out[CHANNEL_COL].copy()

    def _map_one(v: object) -> object:
        if not str(v).strip():
            return v
        return ch_map.get(str(v).strip(), str(v).strip())

    mapped = out[CHANNEL_COL].map(_map_one)
    if preserve_channel_mask is not None and preserve_channel_mask.any():
        keep = preserve_channel_mask.reindex(out.index, fill_value=False).astype(bool)
        mapped = orig.where(keep, mapped)
        n_keep = int(keep.sum())
        if n_keep:
            _LOG.info("渠道映射保留原始渠道名（跳过替换）: %d 行", n_keep)
    out[CHANNEL_COL] = mapped
    return out
