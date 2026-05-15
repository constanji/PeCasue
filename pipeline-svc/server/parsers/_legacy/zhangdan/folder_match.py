"""账单文件夹名 → 银行 key 匹配（轻量，无 OCR / pandas）。

原为 Allline ``zhangdan_match_rules`` 的职责：**不参与执行**，仅供预扫与
``zhangdan.all`` 共用一套规则，避免 ``channel_prescan`` 导入整张 ``all.py`` 时顺带装载 ``ibc`` 等重型依赖。
"""

from __future__ import annotations

# 顺序必须与 ``zhangdan.all.BANK_SCRIPTS`` 插入顺序一致（优先匹配靠前）。
BANK_KEYS: tuple[str, ...] = (
    "banking_circle",
    "barclays",
    "citi",
    "db",
    "ewb",
    "ibc",
    "jpm",
    "monoova",
    "scb",
    "xendit",
)

# 文件夹名除包含 bank_key 外，可额外匹配这些子串（小写比较 ASCII；中文等原样 in）
BANK_FOLDER_ALIASES: dict[str, tuple[str, ...]] = {
    "ewb": ("华美", "east west", "eastwest", "east-west"),
}


def folder_matches_bank(folder_name: str, bank_key: str) -> bool:
    fn_lo = folder_name.lower()
    fn_ns = fn_lo.replace(" ", "")
    # 「BC账单」= Banking Circle（非 Barclays）；勿用子串误匹配 ibc 等
    if bank_key == "banking_circle" and fn_ns == "bc账单":
        return True
    if bank_key in fn_lo:
        return True
    for pat in BANK_FOLDER_ALIASES.get(bank_key, ()):
        if pat in folder_name or pat.lower() in fn_lo:
            return True
    return False


def first_matching_bank_key(folder_name: str) -> str | None:
    for bank_key in BANK_KEYS:
        if folder_matches_bank(folder_name, bank_key):
            return bank_key
    return None

