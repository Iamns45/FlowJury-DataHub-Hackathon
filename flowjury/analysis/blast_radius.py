"""Conservative retirement blast-radius analysis over DataHub evidence."""

from __future__ import annotations

import json
import re
from collections import Counter
from typing import Any, Dict, Iterable


_URN_PATTERN = re.compile(r"urn:li:([A-Za-z]+):[^\s\"'<>]+")
_HIDDEN_SINK_PATTERNS = {
    "Kafka or event stream": ("producer.produce", "kafka", "publish(", "send_message("),
    "online feature store": ("featurestore", "materialize_incremental", "redis"),
    "reverse ETL or SaaS API": ("hightouch", "census", "salesforce", "hubspot", "trigger_sync"),
    "object/file delivery": ("s3://", "gcs://", "write_csv", "export_file", "sftp"),
    "operational deletion API": ("delete_subject", "delete_contact", "expire_subject"),
    "webhook or HTTP API": ("requests.post", "httpx.post", "webhook", "client.post"),
}


def _serialize(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump()
    elif hasattr(value, "dict") and callable(value.dict):
        value = value.dict()
    try:
        return json.dumps(value, default=str, sort_keys=True)
    except Exception:
        return str(value)


def _entity_type(urn: str) -> str:
    match = _URN_PATTERN.search(urn)
    return match.group(1) if match else "unknown"


def _normalize_urn(urn: str) -> str:
    return str(urn).rstrip(",)]}")


def summarize_blast_radius(
    output_urns: Iterable[str],
    lineage_by_output: Dict[str, Any],
    dag_source: str,
    protected: bool,
) -> dict:
    """Return known impact and preserve uncertainty when no impact is found."""
    output_set = {_normalize_urn(urn) for urn in output_urns}
    lineage_text = _serialize(lineage_by_output)
    discovered = {
        _normalize_urn(match.group(0)) for match in _URN_PATTERN.finditer(lineage_text)
    } - output_set
    types = Counter(_entity_type(urn) for urn in discovered)

    lowered_source = (dag_source or "").casefold()
    hidden_sinks = [
        label
        for label, markers in _HIDDEN_SINK_PATTERNS.items()
        if any(marker.casefold() in lowered_source for marker in markers)
    ]
    blockers = []
    if protected:
        blockers.append("normalized compliance/retention protection")
    if discovered:
        blockers.append(f"{len(discovered)} cataloged downstream asset(s)")
    if hidden_sinks:
        blockers.append("executable source contains non-catalog consumer signals")

    return {
        "simulation": "pipeline schedule disabled; outputs stop refreshing",
        "output_count": len(output_set),
        "cataloged_downstream_assets": len(discovered),
        "asset_types": dict(sorted(types.items())),
        "sample_downstream_urns": sorted(discovered)[:12],
        "hidden_sink_signals": hidden_sinks,
        "protected": bool(protected),
        "retirement_blockers": blockers,
        "safe_to_retire": False if blockers else None,
        "conclusion": ("BLOCKED_BY_KNOWN_IMPACT" if blockers else "INCONCLUSIVE_NO_KNOWN_IMPACT"),
        "safety_note": "No known impact is not proof of safety; missing lineage remains UNKNOWN.",
    }
