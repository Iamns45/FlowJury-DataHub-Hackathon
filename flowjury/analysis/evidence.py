"""Collect normalized DataHub evidence and provide the deterministic baseline.

Run live against DataHub:
    export DATAHUB_GMS_URL=http://localhost:8080
    export DATAHUB_GMS_TOKEN=<PAT>
    python -m flowjury.analysis.evidence

Validate the classifier logic offline (no DataHub needed):
    python -m flowjury.analysis.evidence --selftest
"""

from __future__ import annotations

import os
import sys
import time
from itertools import combinations
from statistics import median
from typing import Dict, List

from flowjury.domain.models import Evidence, Finding, Signal, Verdict

# --------------------------------------------------------------------------- #
# Tunables
# --------------------------------------------------------------------------- #
NEW_DAYS = 14  # < this much history => too new to judge
DOWNSHIFT_MAX_QPD = 3  # frequent schedule + <= this many reads/day => downshift
TRIM_MIN_COLS = 8  # only consider trimming reasonably wide tables
TRIM_READ_FRACTION = 0.5  # < this fraction of columns read => trim
RUNAWAY_MULT = 3.0  # in-flight run > baseline * this => runaway
RUNAWAY_MIN_MIN = 45  # ...and longer than this many minutes (ignore tiny jobs)
FAILURE_RATE_THRESH = 0.25  # >= this fraction of runs failing => fix/fold
PROTECT_TAGS = {"compliance", "retention", "pii-retention", "sox", "gdpr"}
EXTERNAL_TAGS = {"has-external-sink", "reverse-etl", "external-export"}


def is_frequent(cron: str | None) -> bool:
    """True if the schedule runs many times per day (hourly or sub-hourly)."""
    if not cron:
        return False
    parts = cron.split()
    if len(parts) < 5:
        return False
    hour = parts[1]
    return hour == "*" or hour.startswith("*/") or "/" in hour or "," in hour


def _confidence(ev: Evidence, base: float = 0.9) -> float:
    penalty = sum(
        0.15 for s in (ev.has_consumers, ev.queried, ev.owner_active) if s == Signal.UNKNOWN
    )
    return round(max(0.3, min(0.95, base - penalty)), 2)


def classify(ev: Evidence) -> Finding:
    """Pure function: deterministic facts in, a verdict + reasons out.

    Order matters — safety vetoes and 'unknown' guards run before any
    recommendation to remove or change anything.
    """
    # 1. Compliance/retention: never propose removal, regardless of usage.
    if ev.protected:
        return Finding(
            ev.pipeline,
            Verdict.PROTECT,
            0.99,
            ["Tagged compliance/retention — protected from removal."],
            ev,
        )

    # 2. Runaway: a run is executing right now, far past its baseline — burning compute.
    if (
        ev.running_now == Signal.TRUE
        and ev.current_runtime_min
        and ev.baseline_min
        and ev.current_runtime_min > ev.baseline_min * RUNAWAY_MULT
        and ev.current_runtime_min > RUNAWAY_MIN_MIN
    ):
        return Finding(
            ev.pipeline,
            Verdict.RUNAWAY,
            0.9,
            [
                f"A run has been executing {ev.current_runtime_min:.0f} min vs a "
                f"~{ev.baseline_min:.0f} min baseline — kill the stuck execution "
                f"(keep the schedule)."
            ],
            ev,
        )

    # 3. Repeated silent failures: fix or fold.
    if ev.failure_rate is not None and ev.failure_rate >= FAILURE_RATE_THRESH:
        return Finding(
            ev.pipeline,
            Verdict.FIX_OR_FOLD,
            round(min(0.9, 0.6 + ev.failure_rate / 2), 2),
            [
                f"{ev.failure_rate * 100:.0f}% of recent runs failed — likely broken "
                f"and unnoticed. Fix or retire."
            ],
            ev,
        )

    # 4. Redundancy (decided by the cross-pipeline pass).
    if ev.redundant_of:
        return Finding(
            ev.pipeline,
            Verdict.REDUNDANT,
            0.75,
            [
                f"Duplicates {ev.redundant_of}: same inputs + near-identical "
                f"output schema, but fewer consumers."
            ],
            ev,
        )

    # 5. Too new to judge: low usage is expected, not evidence of abandonment.
    if ev.usage_available and 0 < ev.usage_days < NEW_DAYS:
        return Finding(
            ev.pipeline,
            Verdict.KEEP,
            0.7,
            [f"Only ~{ev.usage_days}d of history — too new to judge."],
            ev,
        )

    # 6. External or uncataloged consumers can make a pipeline look orphaned.
    if ev.external_sink and ev.queried != Signal.TRUE:
        return Finding(
            ev.pipeline,
            Verdict.UNKNOWN,
            0.5,
            [
                "Writes to an external/reverse-ETL sink the catalog cannot see; "
                "confirm the destination with the owner before proposing retirement."
            ],
            ev,
        )

    # 7. Missing usage coverage means unknown, not unused.
    if not ev.usage_available:
        return Finding(
            ev.pipeline,
            Verdict.UNKNOWN,
            0.4,
            ["No usage data for outputs — unknown, not unused. " "Insufficient evidence to act."],
            ev,
        )

    # 8. A fully evidenced orphan is a retirement candidate.
    if ev.has_consumers == Signal.FALSE and ev.queried == Signal.FALSE:
        conf = _confidence(ev, base=0.85)
        reasons = [
            "No downstream consumers in the catalog.",
            "Zero queries against outputs in the observed window.",
        ]
        if ev.owner_active == Signal.FALSE:
            conf = round(min(0.95, conf + 0.07), 2)
            reasons.append("Owner account is inactive (likely left).")
        return Finding(ev.pipeline, Verdict.KILL, conf, reasons, ev)

    # 9. Excessive schedule frequency is a downshift opportunity.
    if (
        is_frequent(ev.schedule)
        and ev.has_consumers == Signal.TRUE
        and ev.max_queries_per_day <= DOWNSHIFT_MAX_QPD
    ):
        return Finding(
            ev.pipeline,
            Verdict.DOWNSHIFT,
            0.7,
            [
                f"Runs frequently ({ev.schedule}) but outputs are read at most "
                f"{ev.max_queries_per_day}x/day — reduce cadence."
            ],
            ev,
        )

    # 10. Unused output columns are a trimming opportunity.
    if (
        ev.columns_read is not None
        and ev.output_columns
        and ev.output_columns >= TRIM_MIN_COLS
        and (ev.columns_read / ev.output_columns) < TRIM_READ_FRACTION
    ):
        return Finding(
            ev.pipeline,
            Verdict.TRIM,
            0.7,
            [
                f"Builds {ev.output_columns} columns but only {ev.columns_read} "
                f"are read downstream — drop the rest."
            ],
            ev,
        )

    # 11. Otherwise the pipeline is healthy.
    return Finding(
        ev.pipeline,
        Verdict.KEEP,
        _confidence(ev),
        ["Live consumers and recent queries — actively used."],
        ev,
    )


def detect_redundancy(evidences: List[Evidence]) -> None:
    """Flag near-duplicate pipelines (mutates .redundant_of on the weaker one)."""

    def jac(a, b):
        sa, sb = set(a), set(b)
        return len(sa & sb) / len(sa | sb) if (sa or sb) else 0.0

    for a, b in combinations(evidences, 2):
        if not (a.inputs and b.inputs and a.output_cols and b.output_cols):
            continue
        if (
            jac(a.inputs, b.inputs) >= 0.8
            and jac(a.output_cols, b.output_cols) >= 0.8
            and a.domain != b.domain
        ):
            weaker, stronger = (a, b) if a.consumer_count <= b.consumer_count else (b, a)
            weaker.redundant_of = stronger.pipeline


# --------------------------------------------------------------------------- #
# Live evidence gathering
# --------------------------------------------------------------------------- #
def _apply_run_history(ev: Evidence, records: List[dict]) -> None:
    """Derive baseline duration, failure rate, and in-flight status from runs."""
    if not records:
        return
    ev.runs_available = True
    completed = [r for r in records if "COMPLETE" in r["status"]]
    open_runs = [
        r for r in records if "STARTED" in r["status"]
    ]  # latest event STARTED => still running

    durations = [r["duration_ms"] for r in completed if r.get("duration_ms")]
    if durations:
        ev.baseline_min = round(median(durations) / 60000, 1)
    if completed:
        fails = [r for r in completed if r["result"] and "FAIL" in r["result"]]
        ev.failure_rate = round(len(fails) / len(completed), 2)
    if open_runs:
        ev.running_now = Signal.TRUE
        latest_start = max(r["start_ms"] for r in open_runs)
        ev.current_runtime_min = round((int(time.time() * 1000) - latest_start) / 60000, 1)
    else:
        ev.running_now = Signal.FALSE


def gather_evidence(client, job_urn: str) -> Evidence:
    flow_urn = client.flow_urn_of(job_urn)
    info = client.flow_info(flow_urn)
    name = info.get("name") or job_urn.split(",")[-2]
    tags = set(client.tags(flow_urn)) | set(client.tags(job_urn))
    inputs, outputs = client.io(job_urn)

    ev = Evidence(
        pipeline=name,
        schedule=info.get("schedule"),
        domain=client.domain(flow_urn),
        protected=bool(tags & PROTECT_TAGS),
        external_sink=bool(tags & EXTERNAL_TAGS),
        owner_active=client.owner_activity(flow_urn),
        inputs=[str(u) for u in inputs],
    )

    total_queries = 0
    for i, ds in enumerate(outputs):
        ds = str(ds)
        cons = client.consumers(ds)
        ev.consumer_count += len(cons)

        usage = client.usage(ds)
        if usage:
            ev.usage_available = True
            ev.usage_days = max(ev.usage_days, len(usage))
            for u in usage:
                q = u.totalSqlQueries or 0
                total_queries += q
                ev.max_queries_per_day = max(ev.max_queries_per_day, q)
            read_fields = {
                fc.fieldPath for u in usage for fc in (u.fieldCounts or []) if (fc.count or 0) > 0
            }
            if any(u.fieldCounts for u in usage):
                ev.columns_read = len(read_fields)

        fields = client.schema_fields(ds)
        if fields and (ev.output_columns is None or len(fields) > ev.output_columns):
            ev.output_columns = len(fields)
            ev.output_cols = fields

    ev.has_consumers = Signal.TRUE if ev.consumer_count > 0 else Signal.FALSE
    if not ev.usage_available:
        ev.queried = Signal.UNKNOWN
    else:
        ev.queried = Signal.TRUE if total_queries > 0 else Signal.FALSE

    _apply_run_history(ev, client.run_records(job_urn))
    return ev


def run_live() -> int:
    from flowjury.integrations.datahub.client import FlowJuryClient

    gms = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080")
    token = os.environ.get("DATAHUB_GMS_TOKEN")
    client = FlowJuryClient(gms, token)

    jobs = client.list_pipeline_jobs()
    if not jobs:
        print("No pipelines found. Did you run scripts.seed_demo_catalog against DataHub?")
        return 1

    evs = [gather_evidence(client, j) for j in jobs]
    detect_redundancy(evs)
    findings = sorted((classify(e) for e in evs), key=lambda f: f.verdict.value)
    _print_table(findings)
    return 0


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #
def _print_table(findings: List[Finding]) -> None:
    print(f"\n{'pipeline':<32} {'verdict':<11} {'conf':<6} reason")
    print("-" * 100)
    for f in findings:
        print(f"{f.pipeline:<32} {f.verdict.value:<11} {f.confidence:<6} {f.reasons[0]}")
    counts: Dict[str, int] = {}
    for f in findings:
        counts[f.verdict.value] = counts.get(f.verdict.value, 0) + 1
    print("\nsummary:", ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))


# --------------------------------------------------------------------------- #
# Offline self-test: fixtures mirroring the 13 seeded pipelines
# --------------------------------------------------------------------------- #
def _fixtures() -> List[tuple]:
    T, F, U = Signal.TRUE, Signal.FALSE, Signal.UNKNOWN
    return [
        # (Evidence, expected verdict)
        (
            Evidence(
                "daily_revenue",
                schedule="0 3 * * *",
                domain="finance",
                has_consumers=T,
                queried=T,
                max_queries_per_day=18,
                owner_active=T,
                usage_available=True,
                usage_days=30,
                consumer_count=1,
            ),
            Verdict.KEEP,
        ),
        (
            Evidence(
                "customer_360_daily",
                schedule="0 4 * * *",
                domain="growth",
                has_consumers=T,
                queried=T,
                max_queries_per_day=40,
                owner_active=T,
                usage_available=True,
                usage_days=30,
                consumer_count=1,
            ),
            Verdict.KEEP,
        ),
        (
            Evidence(
                "nightly_marketing_export",
                schedule="0 2 * * *",
                domain="marketing",
                has_consumers=F,
                queried=F,
                owner_active=F,
                usage_available=True,
                usage_days=30,
                consumer_count=0,
            ),
            Verdict.KILL,
        ),
        (
            Evidence(
                "hourly_inventory_snapshot",
                schedule="0 * * * *",
                domain="data-platform",
                has_consumers=T,
                queried=T,
                max_queries_per_day=1,
                owner_active=T,
                usage_available=True,
                usage_days=30,
                consumer_count=1,
            ),
            Verdict.DOWNSHIFT,
        ),
        (
            Evidence(
                "legacy_daily_revenue",
                schedule="0 1 * * *",
                domain="finance",
                has_consumers=T,
                queried=T,
                max_queries_per_day=1,
                owner_active=T,
                usage_available=True,
                usage_days=30,
                consumer_count=1,
                redundant_of="revenue_features_daily",
            ),
            Verdict.REDUNDANT,
        ),
        (
            Evidence(
                "revenue_features_daily",
                schedule="30 1 * * *",
                domain="ml-platform",
                has_consumers=T,
                queried=T,
                max_queries_per_day=30,
                owner_active=T,
                usage_available=True,
                usage_days=30,
                consumer_count=3,
            ),
            Verdict.KEEP,
        ),
        (
            Evidence(
                "wide_user_profile_build",
                schedule="0 2 * * *",
                domain="ml-platform",
                has_consumers=T,
                queried=T,
                max_queries_per_day=12,
                owner_active=T,
                usage_available=True,
                usage_days=30,
                consumer_count=1,
                output_columns=15,
                columns_read=4,
            ),
            Verdict.TRIM,
        ),
        (
            Evidence(
                "quarterly_compliance_close",
                schedule="0 0 1 */3 *",
                domain="finance",
                has_consumers=F,
                queried=F,
                owner_active=T,
                protected=True,
                usage_available=True,
                usage_days=30,
            ),
            Verdict.PROTECT,
        ),
        (
            Evidence(
                "nightly_hightouch_sync",
                schedule="0 2 * * *",
                domain="marketing",
                has_consumers=F,
                queried=F,
                owner_active=T,
                external_sink=True,
                usage_available=True,
                usage_days=30,
            ),
            Verdict.UNKNOWN,
        ),
        (
            Evidence(
                "ab_test_metrics_daily",
                schedule="0 6 * * *",
                domain="growth",
                has_consumers=F,
                queried=U,
                owner_active=T,
                usage_available=False,
                usage_days=0,
            ),
            Verdict.UNKNOWN,
        ),
        (
            Evidence(
                "new_signup_features",
                schedule="0 2 * * *",
                domain="ml-platform",
                has_consumers=T,
                queried=T,
                max_queries_per_day=3,
                owner_active=T,
                usage_available=True,
                usage_days=5,
                consumer_count=1,
            ),
            Verdict.KEEP,
        ),
        # Run-history fixtures validate repeated failure and active runaway detection.
        (
            Evidence(
                "fraud_scoring_batch",
                schedule="0 5 * * *",
                domain="risk",
                has_consumers=T,
                queried=T,
                max_queries_per_day=5,
                owner_active=T,
                usage_available=True,
                usage_days=30,
                consumer_count=1,
                runs_available=True,
                running_now=F,
                baseline_min=15.0,
                failure_rate=0.33,
            ),
            Verdict.FIX_OR_FOLD,
        ),
        (
            Evidence(
                "realtime_events_agg",
                schedule="0 * * * *",
                domain="growth",
                has_consumers=T,
                queried=T,
                max_queries_per_day=50,
                owner_active=T,
                usage_available=True,
                usage_days=30,
                consumer_count=1,
                runs_available=True,
                running_now=T,
                baseline_min=18.0,
                current_runtime_min=360.0,
                failure_rate=0.0,
            ),
            Verdict.RUNAWAY,
        ),
    ]


def run_selftest() -> int:
    rows = _fixtures()
    ok = True
    print(f"{'pipeline':<32} {'expected':<11} {'got':<11} {'conf':<6} result")
    print("-" * 80)
    for ev, expected in rows:
        f = classify(ev)
        passed = f.verdict == expected
        ok = ok and passed
        print(
            f"{ev.pipeline:<32} {expected.value:<11} {f.verdict.value:<11} "
            f"{f.confidence:<6} {'PASS' if passed else 'FAIL <---'}"
        )
    print("\n" + ("ALL PASS ✅" if ok else "FAILURES ❌"))
    return 0 if ok else 1


def main() -> int:
    if "--selftest" in sys.argv:
        return run_selftest()
    return run_live()


if __name__ == "__main__":
    raise SystemExit(main())
