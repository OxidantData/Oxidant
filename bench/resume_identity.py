"""Bind TPC Connect resume records to run identity (OxidantData/Oxidant#188)."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import qualify_sql


def source_sha_for(path: Path) -> str:
    """Bind the caller and its loaded local qualification/resume helpers.

    This is harness identity, not an engine build or dataset manifest. The
    versioned digest intentionally does not match retired runner-only hashes.
    """
    digest = hashlib.sha256(b"tpc-connect-source-v2\0")
    for name, source in (
        ("runner", path),
        ("qualify_sql", Path(qualify_sql.__file__)),
        ("resume_identity", Path(__file__)),
    ):
        digest.update(name.encode() + b"\0")
        digest.update(hashlib.sha256(source.read_bytes()).digest())
    return digest.hexdigest()


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


def can_reuse(
    prev: dict[str, Any] | None, *, sql_sha: str, transformed_sha: str
) -> bool:
    if not prev:
        return False
    if prev.get("error"):
        return False
    if prev.get("hot_s") is None and prev.get("elapsed_s") is None:
        return False
    if prev.get("sql_sha256") != sql_sha:
        return False
    if prev.get("original_sha256", sql_sha) != sql_sha:
        return False
    # A same-source compatibility row may omit the extra provenance fields only
    # when transformation is a byte-for-byte no-op. Keep that row unchanged; do
    # not invent a transformed hash for an old timing. Q16 and DS intervals do
    # transform, and therefore always require an explicit matching hash.
    return prev.get("transformed_sha256", sql_sha) == transformed_sha
