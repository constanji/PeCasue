"""Scan a user-selected folder for QuickBI exports and CitiHK data layouts."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ui_services.quickbi_service import count_csv_data_rows


def _usable_file(p: Path) -> bool:
    return p.is_file() and not p.name.startswith("~$")


def _newest(paths: list[Path]) -> Path | None:
    ok = [p for p in paths if _usable_file(p)]
    if not ok:
        return None
    return max(ok, key=lambda x: x.stat().st_mtime)


def _iter_data_files(root: Path):
    """递归枚举 extracted 下的表格文件（与 PeCause ``list_source_files`` 一致，避免仅扫一层漏文件）。"""
    if not root.is_dir():
        return
    for p in sorted(root.rglob("*")):
        if not _usable_file(p):
            continue
        try:
            rel = p.relative_to(root).as_posix()
        except ValueError:
            continue
        if "__MACOSX" in Path(rel).parts or p.name.startswith("._"):
            continue
        suf = p.suffix.lower()
        if suf not in (".xlsx", ".xls", ".xlsm", ".csv"):
            continue
        yield p


def _classify_quickbi_bucket(raw: str, n: str) -> str | None:
    """QuickBI：优先按中文「入金 / 出金 / VA」识别，并兼容历史英文片段。"""
    if "入金" in raw or "finance_channel_inbound" in n or ("inbound_sm" in n and "quickbi" in n):
        return "inbound"
    if "出金" in raw or "finance_channel_outbound" in n or ("outbound_sm" in n and "quickbi" in n):
        return "outbound"
    if (
        "finance_channel_valid_va" in n
        or ("valid_va" in n and "quickbi" in n)
        or "vaads_quickbi" in n
        or "va_ads_quickbi" in n
        or ("VA" in raw and "quickbi" in n)
    ):
        return "va"
    return None


def _quickbi_collect(root: Path) -> tuple[list[Path], list[Path], list[Path]]:
    inbound: list[Path] = []
    outbound: list[Path] = []
    va: list[Path] = []
    for p in _iter_data_files(root):
        bucket = _classify_quickbi_bucket(p.name, p.name.lower())
        if bucket == "inbound":
            inbound.append(p)
        elif bucket == "outbound":
            outbound.append(p)
        elif bucket == "va":
            va.append(p)
    return inbound, outbound, va


def _count_selected_quickbi_rows(picked: Path | None) -> int | None:
    """估算选用 CSV 的数据行数（不含表头）。"""
    if picked is None or picked.suffix.lower() != ".csv" or not picked.is_file():
        return None
    return count_csv_data_rows(picked)


def _csv_only(paths: list[Path]) -> list[Path]:
    return [p for p in paths if p.suffix.lower() == ".csv"]


def _looks_like_quickbi_export(raw: str, n: str) -> bool:
    """用于 CitiHK 宽松匹配时排除 QuickBI 三表文件名。"""
    if "finance_channel" in n and "quickbi" in n:
        return True
    if "入金" in raw or "出金" in raw:
        return True
    if "vaads_quickbi" in n or "va_ads_quickbi" in n:
        return True
    if "VA" in raw and "quickbi" in n:
        return True
    return False


def _pick_quickbi_csv_only(paths: list[Path]) -> Path | None:
    """仅选用 CSV（修改时间最新的一份）；无 CSV 则返回 None（不回退 xlsx）。"""
    return _newest(_csv_only(paths))


def _citihk_legacy_signals(d: Path) -> bool:
    """历史约定：2-Inbound*、4outbound、资金流 slip（仅当前目录一层）。"""
    if not d.is_dir():
        return False
    for p in d.iterdir():
        if not _usable_file(p):
            continue
        pl, suf = p.name.lower(), p.suffix.lower()
        if suf not in (".xlsx", ".xls", ".xlsm", ".csv"):
            continue
        if pl.startswith("2-inbound"):
            return True
    if (d / "4outbound.xlsx").is_file() or (d / "4outbound.csv").is_file():
        return True
    return any(
        (d / name).is_file()
        for name in (
            "资金流slip.xlsx",
            "4资金流slip.xlsx",
            "资金流slip.csv",
            "4资金流slip.csv",
        )
    )


def _citihk_relaxed_signals(d: Path) -> bool:
    """按 Inbound / Outbound /「资金流」关键字（递归；排除 QuickBI 导出文件名）。"""
    if not d.is_dir():
        return False
    for p in _iter_data_files(d):
        raw = p.name
        n = raw.lower()
        if _looks_like_quickbi_export(raw, n):
            continue
        if "inbound" in n or "outbound" in n:
            return True
        if "资金流" in raw:
            return True
    return False


def _citihk_dir_has_outbound(d: Path) -> bool:
    """与 ``citihk_core.default_outbound_paths`` 所需的文件布局一致（单目录）。"""
    ac, ax = d / "4outbound.csv", d / "4outbound.xlsx"
    slip_c = (d / "资金流slip.csv").is_file() or (d / "4资金流slip.csv").is_file()
    slip_x = (d / "资金流slip.xlsx").is_file() or (d / "4资金流slip.xlsx").is_file()
    if ac.is_file() and slip_c:
        return True
    if ax.is_file() and slip_x:
        return True
    return False


def _citihk_dirs_with_three_inbound(search_root: Path) -> list[Path]:
    """在 ``search_root`` 下递归查找：某目录内恰好 3 份 ``2-Inbound*.csv`` 或 3 份 ``2-Inbound*.xlsx``。"""
    if not search_root.is_dir():
        return []

    def collect(pattern: str) -> dict[Path, list[Path]]:
        m: dict[Path, list[Path]] = defaultdict(list)
        for p in search_root.rglob(pattern):
            if not _usable_file(p) or "__MACOSX" in p.parts:
                continue
            m[p.parent].append(p)
        return m

    csv_parents = [par for par, fs in collect("2-Inbound*.csv").items() if len(fs) == 3]
    if csv_parents:
        return csv_parents
    return [par for par, fs in collect("2-Inbound*.xlsx").items() if len(fs) == 3]


def _best_citihk_workdir(search_root: Path) -> Path | None:
    """优先选「含完整出金+slip」且路径最浅的目录；否则退化为仅有三份 Inbound 的目录。"""
    triple_dirs = _citihk_dirs_with_three_inbound(search_root)
    if not triple_dirs:
        return None
    resolved = [(d, d.resolve()) for d in triple_dirs]
    with_ob = [d for d, r in resolved if _citihk_dir_has_outbound(r)]
    pool = with_ob if with_ob else [d for d, _ in resolved]
    return min(pool, key=lambda p: (len(p.resolve().parts), str(p.resolve())))


def citihk_dir_signals(d: Path) -> bool:
    return _citihk_legacy_signals(d) or _citihk_relaxed_signals(d)


def resolve_citihk_dir(root: Path) -> Path | None:
    """解析 CitiHK 工作目录。

    ``citihk_core`` 只在**单层目录**内查找 ``2-Inbound*``，因此不能把「含 QuickBI + CitiHK」的
    ``allocation_base`` 根路径直接传入：须在子树中定位真正放置三份 Inbound 的文件夹。
    """
    if not root.is_dir():
        return None
    search_order: list[Path] = []
    sub = root / "CITIHK"
    if sub.is_dir():
        search_order.append(sub)
    if root.resolve() not in {p.resolve() for p in search_order}:
        search_order.append(root)

    for base in search_order:
        wd = _best_citihk_workdir(base)
        if wd is not None:
            return wd.resolve()

    return None


@dataclass(frozen=True)
class FolderScanResult:
    folder: Path
    quickbi_inbound: Path | None
    quickbi_outbound: Path | None
    quickbi_va: Path | None
    citihk_dir: Path | None
    warnings: tuple[str, ...]
    quickbi_source_counts: tuple[int, int, int] = (0, 0, 0)
    quickbi_selected_rows: tuple[int | None, int | None, int | None] = (None, None, None)
    scan_notes: tuple[str, ...] = ()


def scan_bases_folder(folder: str | Path) -> FolderScanResult:
    root = Path(folder).expanduser().resolve()
    warns: list[str] = []
    if not root.is_dir():
        return FolderScanResult(root, None, None, None, None, (f"不是有效目录：{root}",))

    inc, ouc, vac = _quickbi_collect(root)
    csv_inc, csv_ouc, csv_va = _csv_only(inc), _csv_only(ouc), _csv_only(vac)
    qi = _pick_quickbi_csv_only(inc)
    qo = _pick_quickbi_csv_only(ouc)
    qv = _pick_quickbi_csv_only(vac)

    if len(csv_inc) > 1:
        warns.append(f"入金：识别到 {len(csv_inc)} 份 CSV，已取最新修改时间的一份。")
    if len(csv_ouc) > 1:
        warns.append(f"出金：识别到 {len(csv_ouc)} 份 CSV，已取最新修改时间的一份。")
    if len(csv_va) > 1:
        warns.append(f"VA：识别到 {len(csv_va)} 份 CSV，已取最新修改时间的一份。")

    if qi is None:
        if inc:
            warns.append("入金：已匹配到文件但缺少 CSV（须上传 .csv，不再使用 xlsx）。")
        else:
            warns.append(
                "未识别到 QuickBI 入金 CSV（文件名需含「入金」或 finance_channel_inbound 等关键字）。"
            )
    if qo is None:
        if ouc:
            warns.append("出金：已匹配到文件但缺少 CSV（须上传 .csv，不再使用 xlsx）。")
        else:
            warns.append(
                "未识别到 QuickBI 出金 CSV（文件名需含「出金」或 finance_channel_outbound 等关键字）。"
            )
    if qv is None:
        if vac:
            warns.append("VA：已匹配到文件但缺少 CSV（须上传 .csv，不再使用 xlsx）。")
        else:
            warns.append(
                "未识别到 QuickBI VA CSV（文件名需含「VA」类 QuickBI 导出或 finance_channel_valid_va 等关键字）。"
            )

    citi = resolve_citihk_dir(root)
    if citi is None:
        warns.append(
            "未识别到 CitiHK 数据目录：请在某一文件夹（推荐 extracted/.../CITIHK/）内放置 "
            "恰好 3 个 2-Inbound*.csv（或 3 个 2-Inbound*.xlsx）、以及 4outbound 与资金流 slip 的约定文件名。"
        )

    ri = _count_selected_quickbi_rows(qi)
    ro = _count_selected_quickbi_rows(qo)
    rv = _count_selected_quickbi_rows(qv)
    scan_notes = (
        (
            f"QuickBI：入金 CSV {len(csv_inc)}，选用约 {ri if ri is not None else '—'} 行；"
            f"出金 CSV {len(csv_ouc)}，选用约 {ro if ro is not None else '—'} 行；"
            f"VA CSV {len(csv_va)}，选用约 {rv if rv is not None else '—'} 行。"
        ),
    )

    return FolderScanResult(
        folder=root,
        quickbi_inbound=qi,
        quickbi_outbound=qo,
        quickbi_va=qv,
        citihk_dir=citi,
        warnings=tuple(warns),
        quickbi_source_counts=(len(csv_inc), len(csv_ouc), len(csv_va)),
        quickbi_selected_rows=(ri, ro, rv),
        scan_notes=scan_notes,
    )


def citihk_files_dataframe(folder: str | Path | None) -> object:
    if folder is None or not str(folder).strip():
        return pd.DataFrame([{"说明": "尚未设置 CitiHK 目录"}])
    d = Path(folder).expanduser().resolve()
    if not d.is_dir():
        return pd.DataFrame([{"说明": f"目录不存在：{d}"}])

    rows: list[dict[str, object]] = []
    stems: dict[str, dict[str, str]] = {}
    for p in sorted(d.glob("2-Inbound*")):
        if not _usable_file(p):
            continue
        st = p.stem
        rec = stems.setdefault(st, {"基名": st, "XLSX": "否", "CSV": "否"})
        if p.suffix.lower() in (".xlsx", ".xls", ".xlsm"):
            rec["XLSX"] = "是"
        elif p.suffix.lower() == ".csv":
            rec["CSV"] = "是"

    for st, rec in sorted(stems.items(), key=lambda x: x[0]):
        xp = d / f"{st}.xlsx"
        cp = d / f"{st}.csv"
        nrows = count_csv_data_rows(cp) if cp.is_file() else None
        rows.append(
            {
                "类型": "入金",
                "基名": st,
                "XLSX": "是" if xp.is_file() else rec["XLSX"],
                "CSV": "是" if cp.is_file() else rec["CSV"],
                "行数": "" if nrows is None else nrows,
            }
        )

    def pair_row(label: str, xlsx_name: str, csv_name: str) -> None:
        x, c = d / xlsx_name, d / csv_name
        nrows = count_csv_data_rows(c) if c.is_file() else None
        rows.append(
            {
                "类型": label,
                "基名": xlsx_name.replace(".xlsx", ""),
                "XLSX": "是" if x.is_file() else "否",
                "CSV": "是" if c.is_file() else "否",
                "行数": "" if nrows is None else nrows,
            }
        )

    pair_row("出金主表", "4outbound.xlsx", "4outbound.csv")
    for slip_x, slip_c in (
        ("资金流slip.xlsx", "资金流slip.csv"),
        ("4资金流slip.xlsx", "4资金流slip.csv"),
    ):
        if (d / slip_x).is_file() or (d / slip_c).is_file():
            pair_row("资金流 slip", slip_x, slip_c)
            break

    if not rows:
        rows.append(
            {
                "类型": "-",
                "基名": "(无 2-Inbound / 4outbound 等)",
                "XLSX": "-",
                "CSV": "-",
                "行数": "-",
            }
        )
    return pd.DataFrame(rows)
