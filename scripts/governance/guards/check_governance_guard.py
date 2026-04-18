#!/usr/bin/env python3
"""Governance guard runner for docs-governance guard IDs."""

from __future__ import annotations

import argparse
import datetime as dt
import re
from pathlib import Path
from typing import Callable

import yaml


ROOT = Path(__file__).resolve().parents[3]
DOC_INDEX = ROOT / "governance/registry/governance-doc-index.yaml"
SLOT_BINDING = ROOT / "governance/baselines/contract-authority-slot-binding.yaml"
ROOT_README = ROOT / "README.md"
DOCS_README = ROOT / "docs/README.md"
GOVERNANCE_README = ROOT / "governance/README.md"
LOG_SHARD_RE = re.compile(
    r"^docs/execution/logs/WORKING-EXECUTION-LOG-\d{4}-\d{2}-\d{2}-[A-Z]\.md$"
)
ISSUES_REPORT_SHARD_RE = re.compile(
    r"^docs/execution/reports/issues/ISSUES-REPORT-\d{4}-\d{2}-\d{2}-[A-Z]\.md$"
)
BOARD_SHARD_RES: dict[str, re.Pattern[str]] = {
    "docs/execution/boards/TASK-BOARD.md": re.compile(
        r"^docs/execution/boards/TASK-BOARD-\d{4}-\d{2}-\d{2}-[A-Z]\.md$"
    ),
    "docs/execution/boards/GAP-BOARD.md": re.compile(
        r"^docs/execution/boards/GAP-BOARD-\d{4}-\d{2}-\d{2}-[A-Z]\.md$"
    ),
    "docs/execution/boards/MULTI-AGENT-BOARD.md": re.compile(
        r"^docs/execution/boards/MULTI-AGENT-BOARD-\d{4}-\d{2}-\d{2}-[A-Z]\.md$"
    ),
    "docs/execution/boards/governance/DOCS-GOVERNANCE-REMEDIATION-BOARD.md": re.compile(
        r"^docs/execution/boards/governance/DOCS-GOVERNANCE-REMEDIATION-BOARD-\d{4}-\d{2}-\d{2}-[A-Z]\.md$"
    ),
}

ALLOWED_STATES = {"draft", "active", "superseded", "archived"}
REPORT_TYPES = ("execution", "readiness", "review", "audit", "issues", "postmortem")


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        raise RuntimeError(f"missing file: {path.relative_to(ROOT)}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError(f"invalid YAML root object: {path.relative_to(ROOT)}")
    return data


def _header_fields(path: Path, max_lines: int = 80) -> dict[str, str]:
    fields: dict[str, str] = {}
    lines = path.read_text(encoding="utf-8").splitlines()
    for line in lines[:max_lines]:
        stripped = line.strip()
        if stripped.startswith("## "):
            break
        match = re.match(r"^([A-Za-z][A-Za-z0-9 _/\-]*):\s*(.+?)\s*$", line)
        if not match:
            continue
        key = match.group(1).strip().lower().replace("_", " ")
        key = re.sub(r"\s+", " ", key)
        fields[key] = match.group(2).strip()
    return fields


def _md_files(path: Path) -> list[Path]:
    if not path.exists():
        return []
    return sorted(p for p in path.rglob("*.md") if p.is_file())


def _iso8601_with_tz(value: str) -> bool:
    return bool(
        re.match(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})$",
            value.strip(),
        )
    )


def _parse_iso8601_with_tz(value: str) -> dt.datetime | None:
    if not _iso8601_with_tz(value):
        return None
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _collect_doc_index() -> list[dict]:
    payload = _load_yaml(DOC_INDEX)
    docs = payload.get("documents")
    if not isinstance(docs, list):
        raise RuntimeError("governance/registry/governance-doc-index.yaml missing 'documents' list")
    return [item for item in docs if isinstance(item, dict)]


def _resolve_report_root() -> Path:
    candidates = (
        ROOT / "docs/execution/reports",
        ROOT / "docs/archive/execution/legacy-roadmap/reports",
    )
    for path in candidates:
        if path.exists():
            return path
    return candidates[0]


def _resolve_board_path(canonical: str, legacy: str) -> Path:
    canonical_path = ROOT / canonical
    if canonical_path.exists():
        header = _header_fields(canonical_path)
        shard_rel = header.get("active board shard", "").strip()
        if shard_rel:
            shard_path = ROOT / shard_rel
            if shard_path.exists():
                return shard_path
        return canonical_path
    legacy_path = ROOT / legacy
    if legacy_path.exists():
        return legacy_path
    return canonical_path


def _resolve_governance_remediation_board_path() -> Path:
    candidates = (
        ROOT / "docs/execution/boards/governance/DOCS-GOVERNANCE-REMEDIATION-BOARD.md",
        ROOT / "docs/archive/execution/governance-remediation/DOCS-GOVERNANCE-REMEDIATION-BOARD-V2.md",
    )
    for path in candidates:
        if path.exists():
            if path.name == "DOCS-GOVERNANCE-REMEDIATION-BOARD.md":
                header = _header_fields(path)
                shard_rel = header.get("active board shard", "").strip()
                if shard_rel:
                    shard_path = ROOT / shard_rel
                    if shard_path.exists():
                        return shard_path
            return path
    return candidates[0]


def _check_one_active_authority_per_domain() -> list[str]:
    findings: list[str] = []
    by_semantic_domain: dict[str, list[str]] = {}
    for item in _collect_doc_index():
        if str(item.get("type", "")).lower() != "authority":
            continue
        if str(item.get("state", "")).lower() != "active":
            continue
        standard_id = str(item.get("standard_id", "")).strip()
        domain = str(item.get("domain", "")).strip() or "__missing_domain__"
        semantic_domain = standard_id or domain
        aid = str(item.get("artifact_id", item.get("path", "<unknown>")))
        by_semantic_domain.setdefault(semantic_domain, []).append(aid)
    for semantic_domain, members in sorted(by_semantic_domain.items()):
        if len(members) > 1:
            findings.append(
                f"semantic domain '{semantic_domain}' has multiple active authority artifacts: {', '.join(members)}"
            )
    return findings


def _check_lifecycle_transition() -> list[str]:
    findings: list[str] = []
    for item in _collect_doc_index():
        aid = str(item.get("artifact_id", item.get("path", "<unknown>")))
        state = str(item.get("state", "")).strip().lower()
        if state not in ALLOWED_STATES:
            findings.append(f"{aid}: invalid state '{state}'")
    return findings


def _check_execution_to_archive_expiry() -> list[str]:
    findings: list[str] = []
    roots = [ROOT / "docs/execution/boards", ROOT / "docs/execution/logs", ROOT / "docs/execution/reports"]
    done_re = re.compile(r"^Status:\s*(done|completed|closed)\b", re.IGNORECASE | re.MULTILINE)
    for base in roots:
        for path in _md_files(base):
            rel = path.relative_to(ROOT).as_posix()
            if "/archive/" in rel:
                continue
            text = path.read_text(encoding="utf-8")
            if done_re.search(text):
                findings.append(f"completed artifact should be archived: {rel}")
    return findings


def _check_supersede_link() -> list[str]:
    findings: list[str] = []
    keys = {"superseded by", "superseded_by", "replaced by", "replaced_by"}
    for item in _collect_doc_index():
        if str(item.get("state", "")).lower() != "superseded":
            continue
        rel = str(item.get("path", "")).strip()
        if not rel:
            findings.append(f"{item.get('artifact_id', '<unknown>')}: missing path")
            continue
        path = ROOT / rel
        if not path.exists():
            findings.append(f"superseded artifact missing file: {rel}")
            continue
        header = _header_fields(path)
        if not any(key in header for key in keys):
            findings.append(f"{rel}: superseded artifact missing supersede link header")
    return findings


def _check_report_set_completeness() -> list[str]:
    findings: list[str] = []
    base = _resolve_report_root()
    if not base.exists():
        return [f"missing reports root: {base.relative_to(ROOT)}"]
    for report_type in REPORT_TYPES:
        d = base / report_type
        if not d.exists():
            findings.append(f"missing report directory: {d.relative_to(ROOT)}")
            continue
        if not _md_files(d):
            findings.append(f"empty report directory: {d.relative_to(ROOT)}")
    return findings


def _check_contract_source_trace() -> list[str]:
    findings: list[str] = []
    payload = _load_yaml(SLOT_BINDING)
    required = ("authority_mode", "authority_repo", "authority_paths", "domain_mapping", "sync_policy")
    for key in required:
        if key not in payload:
            findings.append(f"{SLOT_BINDING.relative_to(ROOT)} missing key '{key}'")
    mode = str(payload.get("authority_mode", "")).strip()
    if mode not in {"external_repo", "local_repo", "hybrid_mirror"}:
        findings.append(f"authority_mode must be external_repo|local_repo|hybrid_mirror: got '{mode}'")
    repo = str(payload.get("authority_repo", "")).strip()
    if not repo:
        findings.append("authority_repo must be non-empty")
    auth_paths = payload.get("authority_paths")
    if not isinstance(auth_paths, list) or not auth_paths:
        findings.append("authority_paths must be non-empty list")
    domain_mapping = payload.get("domain_mapping")
    if not isinstance(domain_mapping, dict):
        findings.append("domain_mapping must be object")
    else:
        for key in ("runtime", "control", "surfaces"):
            if key not in domain_mapping:
                findings.append(f"domain_mapping missing '{key}'")
    sync_policy = payload.get("sync_policy")
    if not isinstance(sync_policy, dict):
        findings.append("sync_policy must be object")
    else:
        sync_mode = str(sync_policy.get("sync_mode", "")).strip()
        if sync_mode not in {"ref", "snapshot", "hybrid_mirror"}:
            findings.append(f"sync_policy.sync_mode invalid: '{sync_mode}'")
        sla = sync_policy.get("sync_window_sla_hours")
        if not isinstance(sla, int) or sla <= 0:
            findings.append("sync_policy.sync_window_sla_hours must be positive integer")
    return findings


def _resolve_active_log_path() -> Path:
    index = _resolve_log_index_path()
    if index.exists():
        header = _header_fields(index)
        shard_rel = header.get("active log shard", "").strip()
        if shard_rel:
            shard_path = ROOT / shard_rel
            if shard_path.exists():
                return shard_path
    legacy = ROOT / "docs/archive/execution/legacy-roadmap/GOVERNANCE-WORKING-EXECUTION-LOG.md"
    if legacy.exists():
        return legacy
    return ROOT / "docs/execution/logs/WORKING-EXECUTION-LOG.md"


def _resolve_log_index_path() -> Path:
    return ROOT / "docs/execution/logs/WORKING-EXECUTION-LOG.md"


def _check_log_entry_timestamp() -> list[str]:
    findings: list[str] = []
    path = _resolve_active_log_path()
    if not path.exists():
        return [f"missing execution log shard: {path.relative_to(ROOT)}"]

    lines = path.read_text(encoding="utf-8").splitlines()
    entry_re = re.compile(r"^\d+\.\s+(.+)$")
    matched = 0
    in_log_entries = False
    for line in lines:
        if line.strip().lower() == "## log entries":
            in_log_entries = True
            continue
        if in_log_entries and line.strip().startswith("## "):
            break
        if not in_log_entries:
            continue
        m = entry_re.match(line.strip())
        if not m:
            continue
        value = m.group(1).strip()
        matched += 1
        if not _iso8601_with_tz(value):
            findings.append(
                f"{path.relative_to(ROOT)} invalid timestamp entry '{value}' "
                "(expected ISO 8601 with timezone)"
            )
    if matched == 0:
        findings.append(f"{path.relative_to(ROOT)} has no numbered log timestamp entries")
    return findings


def _check_report_required_fields() -> list[str]:
    findings: list[str] = []
    base = _resolve_report_root()
    for path in _md_files(base):
        rel = path.relative_to(ROOT).as_posix()
        if rel.endswith("/README.md"):
            continue
        header = _header_fields(path)
        if "status" not in header:
            findings.append(f"{rel} missing header field 'Status'")
        if "owner" not in header:
            findings.append(f"{rel} missing header field 'Owner'")
    return findings


def _check_authority_reference_only() -> list[str]:
    findings: list[str] = []
    scope_files = _md_files(_resolve_report_root())
    scope_files.append(_resolve_active_log_path())
    normative_re = re.compile(r"\b(must|mandatory|forbidden|required)\b", re.IGNORECASE)
    for path in sorted(set(scope_files)):
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        if not normative_re.search(text):
            continue
        header = _header_fields(path)
        ref_only = header.get("reference-only", "").strip().lower()
        has_source = "authority-source" in header
        if ref_only != "true" or not has_source:
            findings.append(
                f"{path.relative_to(ROOT)} includes normative language "
                "without Reference-Only/Authority-Source headers"
            )
    return findings


def _check_semantic_duplication() -> list[str]:
    findings: list[str] = []
    by_standard: dict[str, list[str]] = {}
    for item in _collect_doc_index():
        if str(item.get("type", "")).lower() != "authority":
            continue
        if str(item.get("state", "")).lower() != "active":
            continue
        sid = str(item.get("standard_id", "")).strip()
        aid = str(item.get("artifact_id", item.get("path", "<unknown>")))
        if sid:
            by_standard.setdefault(sid, []).append(aid)
    for sid, artifacts in sorted(by_standard.items()):
        if len(artifacts) > 1:
            findings.append(f"standard_id '{sid}' has duplicated active authority: {', '.join(artifacts)}")
    return findings


def _check_readme_boundary() -> list[str]:
    findings: list[str] = []
    banned_root = ("draft -> active", "superseded -> archived", "guard.docs.")
    banned_docs = ("guard.docs.", "lifecycle transition")
    banned_governance = ("working-execution-log", "reports/")
    deprecated_path_map = {
        "docs/archive/execution/legacy-roadmap/multi-agent-task-board.md": "docs/execution/boards/MULTI-AGENT-BOARD.md",
        "docs/archive/execution/legacy-roadmap/governance-working-execution-log.md": "docs/execution/logs/WORKING-EXECUTION-LOG.md",
        "docs/archive/execution/legacy-roadmap/reports/": "docs/execution/reports/",
    }

    if ROOT_README.exists():
        text = ROOT_README.read_text(encoding="utf-8").lower()
        for token in banned_root:
            if token in text:
                findings.append(f"README.md contains governance-normative token '{token}'")
    if DOCS_README.exists():
        text = DOCS_README.read_text(encoding="utf-8").lower()
        for token in banned_docs:
            if token in text:
                findings.append(f"docs/README.md contains governance-normative token '{token}'")
    if GOVERNANCE_README.exists():
        text = GOVERNANCE_README.read_text(encoding="utf-8").lower()
        for token in banned_governance:
            if token in text:
                findings.append(f"governance/README.md contains execution-content token '{token}'")

    # Path convergence lint for entry docs:
    # old paths are allowed only when clearly marked as legacy/transition context.
    convergence_targets = (ROOT_README, DOCS_README, GOVERNANCE_README, ROOT / "docs/execution/boards/governance/DOCS-GOVERNANCE-REMEDIATION-BOARD.md")
    for path in convergence_targets:
        if not path.exists():
            continue
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            lowered = line.lower()
            for old_path, new_path in deprecated_path_map.items():
                if old_path not in lowered:
                    continue
                if any(token in lowered for token in ("legacy", "migration", "transition")):
                    continue
                findings.append(
                    f"{path.relative_to(ROOT)}:{lineno} uses deprecated path '{old_path}' "
                    f"without legacy marker; prefer '{new_path}'"
                )
    return findings


def _check_cross_layer_coupling() -> list[str]:
    findings: list[str] = []
    for path in _md_files(ROOT / "governance"):
        name = path.name.upper()
        if "REPORT" in name or "LOG" in name:
            findings.append(f"governance should not hold execution/report artifacts: {path.relative_to(ROOT)}")

    for base in (ROOT / "governance", ROOT / "docs"):
        for ext in ("*.py", "*.mjs", "*.sh", "*.ts"):
            for path in base.rglob(ext):
                findings.append(f"non-executable layer contains script file: {path.relative_to(ROOT)}")
    return findings


def _check_authoring_scaffold() -> list[str]:
    findings: list[str] = []
    log_index_path = _resolve_log_index_path()
    log_path = _resolve_active_log_path()
    reports_root = _resolve_report_root()
    required_dirs = (
        ROOT / "docs/strategy",
        ROOT / "docs/reference",
        ROOT / "docs/archive",
        ROOT / "docs/archive/execution",
    )
    for path in required_dirs:
        if not path.exists() or not path.is_dir():
            findings.append(f"missing canonical docs directory: {path.relative_to(ROOT)}")

    task_board_index_path = ROOT / "docs/execution/boards/TASK-BOARD.md"
    gap_board_index_path = ROOT / "docs/execution/boards/GAP-BOARD.md"
    multi_agent_board_index_path = ROOT / "docs/execution/boards/MULTI-AGENT-BOARD.md"
    remediation_board_index_path = ROOT / "docs/execution/boards/governance/DOCS-GOVERNANCE-REMEDIATION-BOARD.md"
    issues_report_index_path = ROOT / "docs/execution/reports/issues/ISSUES-REPORT.md"

    required_paths = (
        task_board_index_path,
        gap_board_index_path,
        multi_agent_board_index_path,
        remediation_board_index_path,
        log_index_path,
        issues_report_index_path,
        reports_root / "README.md",
    )
    for path in required_paths:
        if not path.exists():
            findings.append(f"missing scaffold artifact: {path.relative_to(ROOT)}")
    for report_type in REPORT_TYPES:
        d = reports_root / report_type
        if not d.exists():
            findings.append(f"missing scaffold report type directory: {d.relative_to(ROOT)}")
    if log_path.exists():
        raw = log_path.read_text(encoding="utf-8")
        if "## Entry Template" not in raw:
            findings.append(f"{log_path.relative_to(ROOT)} missing '## Entry Template' section")

    # Execution docs depth budget (V3 §4.1.3): keep active execution surface shallow.
    for path in _md_files(ROOT / "docs/execution"):
        rel = path.relative_to(ROOT)
        depth = len(rel.parts) - 1
        if depth > 4:
            findings.append(
                f"{rel.as_posix()} exceeds execution docs depth budget <=4 "
                f"(current depth={depth})"
            )

    task_board_path = _resolve_board_path("docs/execution/boards/TASK-BOARD.md", "docs/archive/execution/legacy-roadmap/TASK-BOARD.md")
    gap_board_path = _resolve_board_path("docs/execution/boards/GAP-BOARD.md", "docs/archive/execution/legacy-roadmap/GAP-BOARD.md")
    multi_agent_board_path = _resolve_board_path(
        "docs/execution/boards/MULTI-AGENT-BOARD.md",
        "docs/archive/execution/legacy-roadmap/MULTI-AGENT-TASK-BOARD.md",
    )
    remediation_board_path = _resolve_governance_remediation_board_path()

    active_dynamic_contracts: dict[Path, tuple[str, ...]] = {
        task_board_path: (
            "board schema version",
            "board revision",
            "rollover trigger",
            "archive target",
            "archive pattern",
        ),
        gap_board_path: (
            "board schema version",
            "board revision",
            "rollover trigger",
            "archive target",
            "archive pattern",
        ),
        multi_agent_board_path: (
            "board schema version",
            "board revision",
            "rollover trigger",
            "archive target",
            "archive pattern",
        ),
        remediation_board_path: (
            "board schema version",
            "board revision",
            "rollover trigger",
            "archive target",
            "archive pattern",
        ),
        log_index_path: (
            "rollover trigger",
            "archive target",
            "archive pattern",
            "active log shard",
            "shard pattern",
        ),
        issues_report_index_path: (
            "rollover trigger",
            "archive target",
            "archive pattern",
            "active report shard",
            "shard pattern",
        ),
    }
    for path, keys in active_dynamic_contracts.items():
        if not path.exists():
            continue
        header = _header_fields(path)
        for key in keys:
            if key not in header:
                findings.append(f"{path.relative_to(ROOT)} missing dynamic header '{key}'")

        expected_archive_targets = {
            task_board_path: "docs/archive/execution/",
            gap_board_path: "docs/archive/execution/",
            multi_agent_board_path: "docs/archive/execution/",
            remediation_board_path: "docs/archive/execution/governance-remediation/",
            log_index_path: "docs/archive/execution/",
            issues_report_index_path: "docs/archive/execution/",
        }
        if path in expected_archive_targets:
            expected_target = expected_archive_targets[path]
            archive_target = header.get("archive target", "").strip()
            if archive_target != expected_target:
                findings.append(
                    f"{path.relative_to(ROOT)} archive target must be '{expected_target}' "
                    f"(got '{archive_target or '<missing>'}')"
                )

    board_index_contracts = {
        task_board_index_path: "docs/archive/execution/",
        gap_board_index_path: "docs/archive/execution/",
        multi_agent_board_index_path: "docs/archive/execution/",
        remediation_board_index_path: "docs/archive/execution/governance-remediation/",
    }
    for index_path, expected_archive_target in board_index_contracts.items():
        if not index_path.exists():
            continue
        header = _header_fields(index_path)
        for key in ("rollover trigger", "archive target", "archive pattern", "active board shard", "shard pattern"):
            if key not in header:
                findings.append(f"{index_path.relative_to(ROOT)} missing dynamic header '{key}'")
        archive_target = header.get("archive target", "").strip()
        if archive_target and archive_target != expected_archive_target:
            findings.append(
                f"{index_path.relative_to(ROOT)} archive target must be '{expected_archive_target}' "
                f"(got '{archive_target}')"
            )

        active_shard = header.get("active board shard", "").strip()
        index_rel = index_path.relative_to(ROOT).as_posix()
        pattern = BOARD_SHARD_RES.get(index_rel)
        if active_shard:
            if pattern is not None and not pattern.match(active_shard):
                findings.append(
                    f"{index_path.relative_to(ROOT)} Active Board Shard must match board shard pattern"
                )
            if not (ROOT / active_shard).exists():
                findings.append(
                    f"{index_path.relative_to(ROOT)} Active Board Shard target missing: {active_shard}"
                )

    def _extract_limit(raw: str, metric: str) -> int | None:
        match = re.search(rf"\b{re.escape(metric)}\s*=\s*(\d+)\b", raw, re.IGNORECASE)
        if not match:
            return None
        return int(match.group(1))

    def _board_anchor_date(path: Path, header: dict[str, str]) -> dt.date | None:
        phase_value = header.get("active phase", "").strip()
        phase_match = re.search(r"\bPH-(\d{8})-\d+\b", phase_value)
        if phase_match:
            raw = phase_match.group(1)
            try:
                return dt.date(int(raw[0:4]), int(raw[4:6]), int(raw[6:8]))
            except ValueError:
                return None
        return None

    def _count_board_rows(path: Path) -> int:
        lines = path.read_text(encoding="utf-8").splitlines()
        table_rows = [
            line
            for line in lines
            if line.startswith("|") and not re.fullmatch(r"\|[-| ]+\|?", line.strip())
        ]
        # first row is header
        return max(len(table_rows) - 1, 0)

    def _log_entry_timestamps(path: Path) -> list[dt.datetime]:
        out: list[dt.datetime] = []
        in_log_entries = False
        entry_re = re.compile(r"^\d+\.\s+(.+)$")
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped.lower() == "## log entries":
                in_log_entries = True
                continue
            if in_log_entries and stripped.startswith("## "):
                break
            if not in_log_entries:
                continue
            m = entry_re.match(stripped)
            if not m:
                continue
            ts = _parse_iso8601_with_tz(m.group(1).strip())
            if ts is not None:
                out.append(ts)
        return out

    def _count_log_entries(path: Path) -> int:
        count = 0
        in_log_entries = False
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped.lower() == "## log entries":
                in_log_entries = True
                continue
            if in_log_entries and stripped.startswith("## "):
                break
            if not in_log_entries:
                continue
            if re.match(r"^\d+\.\s+\S", stripped):
                count += 1
        return count

    board_paths = {
        task_board_path,
        gap_board_path,
        multi_agent_board_path,
        remediation_board_path,
    }
    log_index_path = _resolve_log_index_path()
    log_path = _resolve_active_log_path()

    if log_index_path.exists():
        index_header = _header_fields(log_index_path)
        active_shard = index_header.get("active log shard", "").strip()
        if not active_shard:
            findings.append(f"{log_index_path.relative_to(ROOT)} missing 'Active Log Shard' header")
        else:
            if not LOG_SHARD_RE.match(active_shard):
                findings.append(
                    f"{log_index_path.relative_to(ROOT)} Active Log Shard must match WORKING-EXECUTION-LOG-YYYY-MM-DD-<PART>.md"
                )
            if not (ROOT / active_shard).exists():
                findings.append(
                    f"{log_index_path.relative_to(ROOT)} Active Log Shard target missing: {active_shard}"
                )

    if issues_report_index_path.exists():
        index_header = _header_fields(issues_report_index_path)
        active_shard = index_header.get("active report shard", "").strip()
        if not active_shard:
            findings.append(f"{issues_report_index_path.relative_to(ROOT)} missing 'Active Report Shard' header")
        else:
            if not ISSUES_REPORT_SHARD_RE.match(active_shard):
                findings.append(
                    f"{issues_report_index_path.relative_to(ROOT)} Active Report Shard must match ISSUES-REPORT-YYYY-MM-DD-<PART>.md"
                )
            if not (ROOT / active_shard).exists():
                findings.append(
                    f"{issues_report_index_path.relative_to(ROOT)} Active Report Shard target missing: {active_shard}"
                )

    for path in board_paths:
        if not path.exists():
            continue
        header = _header_fields(path)
        rollover = header.get("rollover trigger", "")
        max_rows = _extract_limit(rollover, "max_rows")
        age_days = _extract_limit(rollover, "age_days")
        if max_rows is None and age_days is None:
            findings.append(
                f"{path.relative_to(ROOT)} rollover trigger must include max_rows=<int> and/or age_days=<int>"
            )
            continue
        row_count = _count_board_rows(path)
        if max_rows is not None and row_count > max_rows:
            findings.append(
                f"{path.relative_to(ROOT)} exceeds rollover max_rows={max_rows} "
                f"(current rows={row_count}); archive/compact required"
            )
        if age_days is not None:
            anchor_date = _board_anchor_date(path, header)
            if anchor_date is None:
                findings.append(
                    f"{path.relative_to(ROOT)} rollover trigger includes age_days but Active Phase is missing/invalid"
                )
            else:
                age = (dt.date.today() - anchor_date).days
                if age > age_days:
                    findings.append(
                        f"{path.relative_to(ROOT)} exceeds rollover age_days={age_days} "
                        f"(current age={age}d from Active Phase); archive/compact required"
                    )

    if log_path.exists():
        header_source = log_index_path if log_index_path.exists() else log_path
        header = _header_fields(header_source)
        rollover = header.get("rollover trigger", "")
        max_entries = _extract_limit(rollover, "max_entries")
        age_days = _extract_limit(rollover, "age_days")
        if max_entries is None and age_days is None:
            findings.append(
                f"{header_source.relative_to(ROOT)} rollover trigger must include max_entries=<int> and/or age_days=<int>"
            )
        if max_entries is not None:
            entry_count = _count_log_entries(log_path)
            if entry_count > max_entries:
                findings.append(
                    f"{log_path.relative_to(ROOT)} exceeds rollover max_entries={max_entries} "
                    f"(current entries={entry_count}); archive/compact required"
                )
        if age_days is not None:
            timestamps = _log_entry_timestamps(log_path)
            if not timestamps:
                findings.append(
                    f"{log_path.relative_to(ROOT)} rollover trigger includes age_days but log has no parseable timestamps"
                )
            else:
                oldest_day = min(ts.astimezone(dt.timezone.utc).date() for ts in timestamps)
                age = (dt.datetime.now(dt.timezone.utc).date() - oldest_day).days
                if age > age_days:
                    findings.append(
                        f"{log_path.relative_to(ROOT)} exceeds rollover age_days={age_days} "
                        f"(oldest entry age={age}d); archive/compact required"
                    )
    return findings


def _check_lifecycle_header_state_lint() -> list[str]:
    findings: list[str] = []
    for item in _collect_doc_index():
        rel = str(item.get("path", "")).strip()
        state = str(item.get("state", "")).strip().lower()
        aid = str(item.get("artifact_id", rel or "<unknown>"))
        if state not in ALLOWED_STATES:
            findings.append(f"{aid}: invalid registry state '{state}'")
            continue
        if not rel:
            findings.append(f"{aid}: missing path")
            continue
        path = ROOT / rel
        if not path.exists():
            findings.append(f"{aid}: path not found {rel}")
            continue
        header = _header_fields(path)
        status = header.get("status", "").strip().lower()
        if status != state:
            findings.append(f"{rel}: header Status='{status or '<missing>'}' != registry state '{state}'")
    return findings


def _check_hybrid_mirror_sync_freshness() -> list[str]:
    findings: list[str] = []
    payload = _load_yaml(SLOT_BINDING)
    sync = payload.get("sync_policy", {})
    if not isinstance(sync, dict):
        return ["sync_policy must be object"]
    mode = str(sync.get("sync_mode", "")).strip()
    if mode not in {"ref", "snapshot", "hybrid_mirror"}:
        findings.append(f"invalid sync_mode: '{mode}'")
        return findings
    if mode == "ref":
        return findings

    mirrored_at = str(sync.get("mirrored_at", "")).strip()
    if not mirrored_at:
        findings.append("sync_policy.mirrored_at required for snapshot/hybrid_mirror mode")
        return findings
    try:
        when = dt.datetime.fromisoformat(mirrored_at.replace("Z", "+00:00"))
    except ValueError:
        findings.append("sync_policy.mirrored_at must be ISO 8601 datetime")
        return findings
    sla = sync.get("sync_window_sla_hours")
    if not isinstance(sla, int) or sla <= 0:
        findings.append("sync_policy.sync_window_sla_hours must be positive integer")
        return findings
    now = dt.datetime.now(dt.timezone.utc)
    age_hours = (now - when.astimezone(dt.timezone.utc)).total_seconds() / 3600.0
    if age_hours > float(sla):
        findings.append(f"mirror is stale: age_hours={age_hours:.2f} > SLA={sla}")
    return findings


def _check_generated_artifact_drift() -> list[str]:
    findings: list[str] = []
    docs = _collect_doc_index()
    for item in docs:
        mode = str(item.get("maintenance_mode", "manual")).strip().lower()
        if mode not in {"generated", "hybrid"}:
            continue
        aid = str(item.get("artifact_id", item.get("path", "<unknown>")))
        if not str(item.get("generator_id", "")).strip():
            findings.append(f"{aid}: generated/hybrid artifact missing generator_id")
        if not str(item.get("source_digest", "")).strip():
            findings.append(f"{aid}: generated/hybrid artifact missing source_digest")
    return findings


def _check_authority_slot_binding() -> list[str]:
    findings: list[str] = []
    payload = _load_yaml(SLOT_BINDING)
    if payload.get("schema_version") != "contract_authority_slot_binding.v1":
        findings.append("schema_version must be contract_authority_slot_binding.v1")
    expected_path = "governance/baselines/contract-authority-slot-binding.yaml"
    if str(payload.get("binding_artifact_path", "")).strip() != expected_path:
        findings.append("binding_artifact_path must point to governance/baselines/contract-authority-slot-binding.yaml")
    mapping = payload.get("domain_mapping")
    if not isinstance(mapping, dict):
        findings.append("domain_mapping must be object")
        return findings
    for key in ("runtime", "control"):
        if not str(mapping.get(key, "")).strip():
            findings.append(f"domain_mapping.{key} must be non-empty path")
    surfaces = mapping.get("surfaces")
    if not isinstance(surfaces, dict):
        findings.append("domain_mapping.surfaces must be object")
    else:
        if not any(str(v).strip() for v in surfaces.values()):
            findings.append("domain_mapping.surfaces must contain at least one non-empty path")
    return findings


def _check_registry_sync() -> list[str]:
    findings: list[str] = []
    docs = _collect_doc_index()
    indexed_paths: set[str] = set()
    for item in docs:
        rel = str(item.get("path", "")).strip()
        if not rel:
            aid = str(item.get("artifact_id", "<unknown>"))
            findings.append(f"{aid}: missing path")
            continue
        indexed_paths.add(rel)
        if not (ROOT / rel).exists():
            findings.append(f"indexed path not found: {rel}")

    # Discover all markdown files under ROOT that are governance-relevant.
    # Guards scope: README files and docs/ tree.
    candidates: set[Path] = set()
    if ROOT_README.exists():
        candidates.add(ROOT_README)
    if DOCS_README.exists():
        candidates.add(DOCS_README)
    if GOVERNANCE_README.exists():
        candidates.add(GOVERNANCE_README)
    for p in (ROOT / "docs").rglob("*.md"):
        if "/archive/" not in p.relative_to(ROOT / "docs").as_posix():
            candidates.add(p)
    for path in candidates:
        rel = path.relative_to(ROOT).as_posix()
        if rel not in indexed_paths:
            findings.append(f"document not indexed: {rel}")
    return findings


def _check_report_file_naming() -> list[str]:
    findings: list[str] = []
    base = _resolve_report_root()
    if not base.exists():
        return []
    for path in _md_files(base):
        rel = path.relative_to(ROOT).as_posix()
        if rel.endswith("/README.md"):
            continue
        # Check shard naming: execution reports use WORKING-EXECUTION-LOG, boards use TASK-BOARD, etc.
        # Non-shard reports (e.g. README, index) are exempt.
        # Board shards already validated by board patterns; report shards use ISSUES-REPORT pattern.
        if "/execution/boards/" in rel or "/execution/logs/" in rel or "/execution/reports/" in rel:
            # Allow index files and canonical pointers; only shard files are validated here.
            pass
        # Additional gate: report files with year-date tokens must match shard pattern or be legacy-marked.
        import re as _re
        shard_year_re = _re.compile(r"\d{4}-\d{2}-\d{2}")
        base_name = path.name
        if shard_year_re.search(base_name):
            is_shard = any(
                pat.match(rel) for pat in (
                    BOARD_SHARD_RES.get(rel.split("/")[0] + "/" + rel.split("/")[1], _re.compile("$^"))
                    for rel in [rel]
                )
            )
            if not is_shard and "archive" not in rel and "legacy" not in rel.lower():
                findings.append(f"{rel}: report file with date token may need shard naming or archive/legacy marker")
    return findings


CHECKS: dict[str, Callable[[], list[str]]] = {
    "guard.registry.sync": _check_registry_sync,
    "guard.docs.report-file-naming": _check_report_file_naming,
    "guard.docs.one-active-authority-per-domain": _check_one_active_authority_per_domain,
    "guard.docs.lifecycle-transition": _check_lifecycle_transition,
    "guard.docs.execution-to-archive-expiry": _check_execution_to_archive_expiry,
    "guard.docs.supersede-link": _check_supersede_link,
    "guard.docs.report-set-completeness": _check_report_set_completeness,
    "guard.contract.source-trace": _check_contract_source_trace,
    "guard.docs.log-entry-timestamp": _check_log_entry_timestamp,
    "guard.docs.report-required-fields": _check_report_required_fields,
    "guard.docs.authority-reference-only": _check_authority_reference_only,
    "guard.docs.semantic-duplication": _check_semantic_duplication,
    "guard.docs.readme-boundary": _check_readme_boundary,
    "guard.docs.cross-layer-coupling": _check_cross_layer_coupling,
    "guard.docs.authoring-scaffold": _check_authoring_scaffold,
    "guard.docs.lifecycle-header-state-lint": _check_lifecycle_header_state_lint,
    "guard.contract.hybrid-mirror-sync-freshness": _check_hybrid_mirror_sync_freshness,
    "guard.docs.generated-artifact-drift": _check_generated_artifact_drift,
    "guard.contract.authority-slot-binding": _check_authority_slot_binding,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one governance guard by guard_id")
    parser.add_argument("--guard-id", required=True, help="guard.<domain>.<slug>")
    args = parser.parse_args()

    fn = CHECKS.get(args.guard_id)
    if fn is None:
        print(f"UNKNOWN_GUARD_ID: {args.guard_id}")
        return 2

    findings = fn()
    if findings:
        for finding in findings:
            print(f"FAIL: {finding}")
        print(f"{args.guard_id}: fail ({len(findings)} finding(s))")
        return 1

    print(f"{args.guard_id}: pass")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
