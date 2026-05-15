from __future__ import annotations

import pandas as pd

from .quickbi_io import CHANNEL_COL, ENTITY_COL


def va_branch_allowed(ch: object, ent: object | None = None) -> bool:
    if ch is None or (isinstance(ch, float) and pd.isna(ch)):
        return False
    s = str(ch).upper().replace(" ", "")
    if "CITI" in s and "MX" in s:
        return True
    if "JPM" in s and "HK" in s:
        return True
    if "JPMSG" in s:
        return True
    if "JPM" in s and "SG" in s:
        return True
    if "JPMUS" in s:
        return True
    if "JPM" in s and "US" in s and "HK" not in s:
        return True
    return False


def filter_va_branches(df: pd.DataFrame) -> pd.DataFrame:
    m = df.apply(
        lambda r: va_branch_allowed(r.get(CHANNEL_COL), r.get(ENTITY_COL)),
        axis=1,
    )
    return df.loc[m].reset_index(drop=True)
