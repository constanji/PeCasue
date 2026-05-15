"""Vendored execution logic for bill (zhangdan) and own-flow pipelines.

Code lives only under ``server/parsers/_legacy/`` — PeCause **does not** depend on
any external ``script/allline`` tree at runtime. Prefer wrapping these modules from
channel-specific ``parser.py`` files rather than editing vendored internals.
"""
