from __future__ import annotations

import logging

import pandas as pd

from .quickbi_io import (
    CHANNEL_COL,
    COLS_INBOUND,
    ENTITY_COL,
    INBOUND_COUNT_COL,
    INBOUND_VOL_COL,
    MONTH_COL,
    OUTBOUND_COUNT_COL,
    OUTBOUND_VOL_COL,
    month_to_yyyymm,
)

_LOG = logging.getLogger("quickbi.line")

# 出金源中此行别实际为入金侧（QuickBI 落在出金表）；与 CHANNEL_DEFAULT_ENTITY 渠道键一致
_PPUS_RECLASS_ENTITY = "PPUS"
_PPUS_RECLASS_CHANNEL = "CITI_US_ACH_DEBIT"

# 由出金并入入金的行：跳过「入金渠道→渠道-分行」替换，保留 QuickBI 原始渠道名（如 CITI_US_ACH_DEBIT），
# 以便与模板 / Excel 公式 VLOOKUP 及汇总按渠道核对一致。
PRESERVE_INBOUND_CHANNEL_MAP_COL = "_preserve_inbound_channel_map"


def norm_entity(s: object) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return str(s).strip().upper()


def norm_channel_for_entity_default(ch: object) -> str:
    """与业务侧渠道写法对齐：大写、去空白，`-` 视为 `_` 便于匹配。"""
    if ch is None or (isinstance(ch, float) and pd.isna(ch)):
        return ""
    return str(ch).strip().upper().replace(" ", "").replace("-", "_")


# 分摊基数规则：主体为空时，按渠道名称补默认主体（与 Excel 模板约定一致的可在此扩展）
CHANNEL_DEFAULT_ENTITY: dict[str, str] = {
    "CITI_US_ACH_DEBIT": "PPUS",
}


def fill_default_entity_for_channels(df: pd.DataFrame) -> pd.DataFrame:
    """若「主体」为空且「渠道名称」命中 CHANNEL_DEFAULT_ENTITY，则写入默认主体。"""
    if df.empty or ENTITY_COL not in df.columns or CHANNEL_COL not in df.columns:
        return df
    out = df.copy()
    ent_norm = out[ENTITY_COL].map(norm_entity)
    ch_norm = out[CHANNEL_COL].map(norm_channel_for_entity_default)
    for ch_key, default_ent in CHANNEL_DEFAULT_ENTITY.items():
        m = ent_norm.eq("") & ch_norm.eq(ch_key)
        if m.any():
            out.loc[m, ENTITY_COL] = default_ent
    return out


def is_pphk_citihk_channel(ch: object) -> bool:
    if ch is None or (isinstance(ch, float) and pd.isna(ch)):
        return False
    s = str(ch).replace("_", "-").strip().upper()
    if s in frozenset({"CITI-HK", "CITIHK"}):
        return True
    return s.startswith("CITI-HK")


def is_pphk_citihk_row(ent: object, ch: object) -> bool:
    return norm_entity(ent) == "PPHK" and is_pphk_citihk_channel(ch)


def uses_prev_month_channel(ch: object) -> bool:
    if ch is None or (isinstance(ch, float) and pd.isna(ch)):
        return False
    u = str(ch).upper()
    if u.startswith("CITI_AU"):
        return False
    return "CITI" in u


def prev_yyyymm(yyyymm: str) -> str:
    y, m = int(yyyymm[:4]), int(yyyymm[4:6])
    m -= 1
    if m == 0:
        m = 12
        y -= 1
    return "%04d%02d" % (y, m)


def expected_month_yyyymm_for_row(ch: object, month_curr: str) -> str:
    if uses_prev_month_channel(ch):
        return prev_yyyymm(month_curr)
    return month_curr


def filter_inbound_outbound(df: pd.DataFrame, *, month_curr: str) -> pd.DataFrame:
    if len(month_curr) != 6 or not month_curr.isdigit():
        raise ValueError("month_curr must be YYYYMM got %r" % month_curr)
    df = fill_default_entity_for_channels(df)
    mcur = month_curr
    mask_ex = ~df.apply(
        lambda r: is_pphk_citihk_row(r.get(ENTITY_COL), r.get(CHANNEL_COL)),
        axis=1,
    )
    df = df.loc[mask_ex].copy()
    mask_mo = df.apply(
        lambda r: month_to_yyyymm(r.get(MONTH_COL))
        == expected_month_yyyymm_for_row(r.get(CHANNEL_COL), mcur),
        axis=1,
    )
    return df.loc[mask_mo].reset_index(drop=True)


def filter_inbound_outbound_all_months(df: pd.DataFrame) -> pd.DataFrame:
    df = fill_default_entity_for_channels(df)
    mask_ex = ~df.apply(
        lambda r: is_pphk_citihk_row(r.get(ENTITY_COL), r.get(CHANNEL_COL)),
        axis=1,
    )
    return df.loc[mask_ex].reset_index(drop=True)


def reclassify_ppus_citi_us_ach_debit_outbound_to_inbound(
    df_in: pd.DataFrame,
    df_out: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """将出金源中 PPUS + CITI_US_ACH_DEBIT 行并入入金（笔数/交易量列按入金口径写入）。"""
    if df_out.empty:
        return df_in, df_out
    need = {
        ENTITY_COL,
        CHANNEL_COL,
        OUTBOUND_COUNT_COL,
        OUTBOUND_VOL_COL,
    }
    if not need.issubset(df_out.columns):
        return df_in, df_out

    ent_n = df_out[ENTITY_COL].map(norm_entity)
    ch_n = df_out[CHANNEL_COL].map(norm_channel_for_entity_default)
    m = ent_n.eq(_PPUS_RECLASS_ENTITY) & ch_n.eq(_PPUS_RECLASS_CHANNEL)
    if not m.any():
        return df_in, df_out

    n_move = int(m.sum())
    _LOG.info(
        "出金→入金重分类: %s+%s 共 %d 行",
        _PPUS_RECLASS_ENTITY,
        _PPUS_RECLASS_CHANNEL,
        n_move,
    )
    moved = df_out.loc[m].copy()
    moved[ENTITY_COL] = _PPUS_RECLASS_ENTITY
    moved = moved.rename(
        columns={
            OUTBOUND_COUNT_COL: INBOUND_COUNT_COL,
            OUTBOUND_VOL_COL: INBOUND_VOL_COL,
        }
    )
    for c in COLS_INBOUND:
        if c not in moved.columns:
            moved[c] = ""
    moved = moved[COLS_INBOUND]
    moved[PRESERVE_INBOUND_CHANNEL_MAP_COL] = True

    d_i = df_in.copy()
    if PRESERVE_INBOUND_CHANNEL_MAP_COL not in d_i.columns:
        d_i[PRESERVE_INBOUND_CHANNEL_MAP_COL] = False
    else:
        d_i[PRESERVE_INBOUND_CHANNEL_MAP_COL] = (
            d_i[PRESERVE_INBOUND_CHANNEL_MAP_COL].fillna(False).astype(bool)
        )

    d_in2 = pd.concat([d_i, moved], ignore_index=True)
    d_out2 = df_out.loc[~m].reset_index(drop=True)
    return d_in2, d_out2


def pop_inbound_channel_preserve_mask(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series | None]:
    """拆出入金渠道映射保护标记列；若无或未启用任一保护行则返回 (df, None)。"""
    if PRESERVE_INBOUND_CHANNEL_MAP_COL not in df.columns:
        return df, None
    pm = df[PRESERVE_INBOUND_CHANNEL_MAP_COL].fillna(False).astype(bool)
    out = df.drop(columns=[PRESERVE_INBOUND_CHANNEL_MAP_COL])
    if not pm.any():
        return out, None
    return out, pm


def filter_va_month_only(df: pd.DataFrame, *, month_curr: str) -> pd.DataFrame:
    if len(month_curr) != 6 or not month_curr.isdigit():
        raise ValueError("month_curr must be YYYYMM got %r" % month_curr)
    df = fill_default_entity_for_channels(df)
    mask = df.apply(lambda r: month_to_yyyymm(r.get(MONTH_COL)) == month_curr, axis=1)
    return df.loc[mask].reset_index(drop=True)


def filter_va_all_months(df: pd.DataFrame) -> pd.DataFrame:
    df = fill_default_entity_for_channels(df)
    return df.reset_index(drop=True)
