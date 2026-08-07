"""Domain types shared by FlowJury's evidence and decision layers."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class Signal(str, Enum):
    """Three-valued evidence. UNKNOWN is NOT False — it means 'we can't see'."""

    TRUE = "TRUE"
    FALSE = "FALSE"
    UNKNOWN = "UNKNOWN"


class Verdict(str, Enum):
    KEEP = "KEEP"  # actively used — leave it alone
    KILL = "KILL"  # orphan — nothing consumes it, no queries
    DOWNSHIFT = "DOWNSHIFT"  # runs too often for how its output is consumed
    REDUNDANT = "REDUNDANT"  # duplicates another pipeline
    TRIM = "TRIM"  # builds columns nobody reads
    RUNAWAY = "RUNAWAY"  # a run is stuck / far past its baseline right now
    FIX_OR_FOLD = "FIX/FOLD"  # fails repeatedly and unnoticed
    PROTECT = "PROTECT"  # compliance/retention — never touch
    UNKNOWN = "UNKNOWN"  # not enough evidence to decide safely


@dataclass
class Evidence:
    """Deterministic facts about one pipeline, gathered from DataHub."""

    pipeline: str
    schedule: Optional[str] = None  # cron string from the flow
    domain: Optional[str] = None
    output_columns: Optional[int] = None  # column count of the primary output
    columns_read: Optional[int] = None  # distinct columns queried; None = no field-usage data
    consumer_count: int = 0
    has_consumers: Signal = Signal.UNKNOWN
    queried: Signal = Signal.UNKNOWN
    max_queries_per_day: int = 0
    owner_active: Signal = Signal.UNKNOWN
    protected: bool = False  # compliance/retention tag present
    external_sink: bool = False  # reverse-ETL / external-sink tag present
    usage_available: bool = False  # did we find ANY usage data at all?
    usage_days: int = 0  # number of usage buckets seen (age proxy)
    # Run-history signals distinguish active, runaway, and repeatedly failing jobs.
    runs_available: bool = False
    running_now: Signal = Signal.UNKNOWN  # is a run currently executing?
    current_runtime_min: Optional[float] = None  # runtime of the in-flight run
    baseline_min: Optional[float] = None  # median duration of completed runs
    failure_rate: Optional[float] = None  # fraction of completed runs that failed
    inputs: List[str] = field(default_factory=list)  # input dataset urns (for redundancy)
    output_cols: List[str] = field(default_factory=list)  # output column names (for redundancy)
    redundant_of: Optional[str] = None  # set by the cross-pipeline pass


@dataclass
class Finding:
    pipeline: str
    verdict: Verdict
    confidence: float
    reasons: List[str] = field(default_factory=list)
    evidence: Optional[Evidence] = None
