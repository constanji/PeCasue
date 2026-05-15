from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .quickbi_io import COLS_INBOUND, COLS_VA, FINAL_BU_COL


@dataclass(frozen=True)
class _LineAttrs:
    region: str = ""
    business_type: str = ""
    line: str = ""
    bu: str = ""


@dataclass(frozen=True)
class FinalBUMapper:
    dept: dict[str, _LineAttrs]
    system: dict[str, _LineAttrs]
    country_region: dict[str, str]
    country_bu: dict[str, str]
    combo_bu: dict[str, str]
    maintainer: dict[str, _LineAttrs]


def _s(v: object) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def _key(v: object) -> str:
    return _s(v).lower()


def _attrs(row: pd.Series, idx_region: int, idx_type: int, idx_line: int, idx_bu: int | None = None) -> _LineAttrs:
    return _LineAttrs(
        region=_s(row.iloc[idx_region]),
        business_type=_s(row.iloc[idx_type]),
        line=_s(row.iloc[idx_line]),
        bu="" if idx_bu is None else _s(row.iloc[idx_bu]),
    )


def _build_attrs_map(
    df: pd.DataFrame,
    key_idx: int,
    idx_region: int,
    idx_type: int,
    idx_line: int,
    idx_bu: int | None = None,
) -> dict[str, _LineAttrs]:
    out: dict[str, _LineAttrs] = {}
    for _, row in df.iterrows():
        k = _key(row.iloc[key_idx])
        if k and k not in out:
            out[k] = _attrs(row, idx_region, idx_type, idx_line, idx_bu)
    return out


def _build_value_map(df: pd.DataFrame, key_idx: int, value_idx: int) -> dict[str, str]:
    out: dict[str, str] = {}
    for _, row in df.iterrows():
        k = _key(row.iloc[key_idx])
        v = _s(row.iloc[value_idx])
        if k and k not in out:
            out[k] = v
    return out


def load_final_bu_mapper(template: Path) -> FinalBUMapper:
    """Read the template mapping sheet used by the original Excel formulas."""
    df = pd.read_excel(
        Path(template),
        sheet_name="mapping",
        dtype=object,
        keep_default_na=False,
        na_filter=False,
    )
    return FinalBUMapper(
        dept=_build_attrs_map(df, 0, 1, 2, 3, 4),  # mapping!A:E
        system=_build_attrs_map(df, 6, 7, 8, 9),  # mapping!G:J
        country_region=_build_value_map(df, 11, 12),  # mapping!L:M
        country_bu=_build_value_map(df, 19, 21),  # mapping!T:V
        combo_bu=_build_value_map(df, 29, 30),  # mapping!AD:AE
        maintainer=_build_attrs_map(df, 35, 36, 37, 38, 39),  # mapping!AJ:AN
    )


def _lookup_attrs(mapping: dict[str, _LineAttrs], value: object) -> _LineAttrs:
    return mapping.get(_key(value), _LineAttrs())


def _lookup_value(mapping: dict[str, str], value: object) -> str:
    return mapping.get(_key(value), "")


def _excel_lookup_or_none(value: object, looked_up: str) -> str:
    return looked_up if _s(value) and looked_up else "无"


def _contains_apac_or_sea(value: object) -> bool:
    s = _s(value).upper()
    return "APAC" in s or "SEA" in s


def _special_apac_bu(row: pd.Series, mapper: FinalBUMapper, dept_col: str, country_col: str) -> str:
    dept = row.get(dept_col)
    country = row.get(country_col)
    if _contains_apac_or_sea(dept):
        return _lookup_attrs(mapper.dept, dept).bu
    if _s(country) == "":
        return "APAC-公共"
    return _lookup_value(mapper.country_bu, country)


_EU_PANDA_COVERAGE_DEPT = "Overseas-Enterprise-Enterprise Coverage-BD(Panda)-EU"


def _in_out_final_bu(row: pd.Series, mapper: FinalBUMapper, *, outbound: bool) -> str:
    dept_col = COLS_INBOUND[4]
    system_col = COLS_INBOUND[5]
    country_col = COLS_INBOUND[6]
    maintainer_col = COLS_INBOUND[9]
    b2b_col = COLS_INBOUND[10]

    dept = row.get(dept_col)
    system = row.get(system_col)
    country = row.get(country_col)
    maintainer = row.get(maintainer_col)

    dept_attrs = _lookup_attrs(mapper.dept, dept)
    maintainer_attrs = _lookup_attrs(mapper.maintainer, maintainer)
    system_attrs = _lookup_attrs(mapper.system, system)

    maint_region = (
        _excel_lookup_or_none(dept, dept_attrs.region)
        if _s(dept)
        else _excel_lookup_or_none(maintainer, maintainer_attrs.region)
    )
    kyc_region = _lookup_value(mapper.country_region, country) if _s(country) else "无"
    system_region = system_attrs.region
    if _s(system) == "CURRENTS" and maint_region == "无":
        region = "International"
    elif maint_region == "无":
        region = system_region if kyc_region == "无" else kyc_region
    else:
        region = maint_region

    maint_type = (
        _excel_lookup_or_none(dept, dept_attrs.business_type)
        if _s(dept)
        else _excel_lookup_or_none(maintainer, maintainer_attrs.business_type)
    )
    business_type = system_attrs.business_type if maint_type == "无" else maint_type

    maint_line = (
        _excel_lookup_or_none(dept, dept_attrs.line)
        if _s(dept)
        else _excel_lookup_or_none(maintainer, maintainer_attrs.line)
    )
    line = system_attrs.line if maint_line == "无" else maint_line

    combo = region + business_type + line
    if combo == "APACSMB业务B2B" and _s(row.get(b2b_col)) == "机构用户":
        combo = "APAC机构业务机构业务"

    if _s(dept) == _EU_PANDA_COVERAGE_DEPT:
        # Reference inbound/outbound cost split treats this EU coverage team as Tiger.
        return "欧美-Tiger"

    # Final BU formula:
    # IF(V="APACSMB业务B2B", APAC special lookup, VLOOKUP(V, mapping!AD:AE, 2, FALSE))
    if combo == "APACSMB业务B2B":
        return _special_apac_bu(row, mapper, dept_col, country_col)
    return _lookup_value(mapper.combo_bu, combo)


def _va_final_bu(row: pd.Series, mapper: FinalBUMapper) -> str:
    dept_col = COLS_VA[4]
    system_col = COLS_VA[5]
    country_col = COLS_VA[6]

    dept = row.get(dept_col)
    system = row.get(system_col)
    country = row.get(country_col)

    dept_attrs = _lookup_attrs(mapper.dept, dept)
    system_attrs = _lookup_attrs(mapper.system, system)

    maint_region = _excel_lookup_or_none(dept, dept_attrs.region) if _s(dept) else "无"
    kyc_region = _lookup_value(mapper.country_region, country) if _s(country) else "无"
    system_region = system_attrs.region
    if _s(system) == "CURRENTS" and maint_region == "无":
        region = "International"
    elif maint_region == "无":
        region = system_region if kyc_region == "无" else kyc_region
    else:
        region = maint_region

    maint_type = _excel_lookup_or_none(dept, dept_attrs.business_type) if _s(dept) else "无"
    business_type = system_attrs.business_type if maint_type == "无" else maint_type

    maint_line = _excel_lookup_or_none(dept, dept_attrs.line) if _s(dept) else "无"
    line = system_attrs.line if maint_line == "无" else maint_line

    combo = region + business_type + line
    if combo == "APACSMB业务B2B":
        return _special_apac_bu(row, mapper, dept_col, country_col)
    return _lookup_value(mapper.combo_bu, combo)


def _fill_missing_final_bu(df: pd.DataFrame, values: pd.Series) -> pd.DataFrame:
    out = df.copy()
    # Recalculate from the template formula every run; source files may contain stale BU values.
    out[FINAL_BU_COL] = values.values
    return out


def enrich_final_bu_from_template(
    *,
    template: Path,
    df_in: pd.DataFrame,
    df_out: pd.DataFrame,
    df_va: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    mapper = load_final_bu_mapper(template)
    in_values = df_in.apply(lambda r: _in_out_final_bu(r, mapper, outbound=False), axis=1)
    out_values = df_out.apply(lambda r: _in_out_final_bu(r, mapper, outbound=True), axis=1)
    va_values = df_va.apply(lambda r: _va_final_bu(r, mapper), axis=1)
    return (
        _fill_missing_final_bu(df_in, in_values),
        _fill_missing_final_bu(df_out, out_values),
        _fill_missing_final_bu(df_va, va_values),
    )
