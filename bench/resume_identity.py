"""Bind TPC Connect resume records to run identity (OxidantData/Oxidant#188)."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any


def source_sha_for(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sql_sha256(sql: str) -> str:
    return hashlib.sha256(sql.encode()).hexdigest()


def run_identity(
    *,
    endpoint: str,
    dataset: str,
    machine: str,
    tries: int,
    source_sha: str,
) -> dict[str, Any]:
    return {
        "source_sha": source_sha,
        "dataset": dataset,
        "endpoint": endpoint,
        "machine": machine,
        "tries": tries,
    }


def reusable_prior(payload: dict[str, Any] | None, identity: dict[str, Any]) -> dict[str, dict]:
    if not payload:
        return {}
    for key in ("source_sha", "dataset", "endpoint", "machine", "tries"):
        if payload.get(key) != identity.get(key):
            return {}
    reused: dict[str, dict] = {}
    for query in payload.get("queries") or []:
        name = query.get("query")
        if isinstance(name, str) and name:
            reused[name] = query
    return reused


def can_reuse(prev: dict[str, Any] | None, *, sql_sha: str) -> bool:
    if not prev:
        return False
    if prev.get("error"):
        return False
    if prev.get("hot_s") is None and prev.get("elapsed_s") is None:
        return False
    return prev.get("sql_sha256") == sql_sha
