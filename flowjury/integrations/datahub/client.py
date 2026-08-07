"""DataHub read adapter used by FlowJury services and agent tools."""

from __future__ import annotations

from typing import List, Optional

from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
from datahub.ingestion.graph.openapi import RelationshipDirection
from datahub.metadata.schema_classes import (
    CorpUserInfoClass,
    DataFlowInfoClass,
    DataJobInputOutputClass,
    DataProcessInstanceRunEventClass,
    DatasetUsageStatisticsClass,
    DomainsClass,
    GlobalTagsClass,
    OwnershipClass,
    SchemaMetadataClass,
)
from datahub.metadata.urns import DataJobUrn

from flowjury.domain.models import Signal

# Dataset lineage uses ``DownstreamOf`` relationships: a downstream dataset
# points to its upstream input. Consumers therefore appear as incoming edges.
_DOWNSTREAM_REL = "DownstreamOf"


def _tag_short(tag_urn: str) -> str:
    return tag_urn.split(":")[-1]


class FlowJuryClient:
    def __init__(self, gms_url: str, token: Optional[str] = None):
        self.graph = DataHubGraph(DatahubClientConfig(server=gms_url, token=token))

    # -- pipelines ---------------------------------------------------------
    def list_pipeline_jobs(self) -> List[str]:
        return list(self.graph.get_urns_by_filter(entity_types=["dataJob"]))

    def io(self, job_urn: str):
        a = self.graph.get_aspect(job_urn, DataJobInputOutputClass)
        if not a:
            return [], []
        return list(a.inputDatasets or []), list(a.outputDatasets or [])

    def flow_urn_of(self, job_urn: str) -> str:
        return str(DataJobUrn.from_string(job_urn).get_data_flow_urn())

    def flow_info(self, flow_urn: str) -> dict:
        a = self.graph.get_aspect(flow_urn, DataFlowInfoClass)
        if not a:
            return {}
        props = dict(a.customProperties or {})
        return {"name": a.name, "schedule": props.get("schedule")}

    def tags(self, urn: str) -> List[str]:
        a = self.graph.get_aspect(urn, GlobalTagsClass)
        if not a or not a.tags:
            return []
        return [_tag_short(t.tag) for t in a.tags]

    def domain(self, urn: str) -> Optional[str]:
        a = self.graph.get_aspect(urn, DomainsClass)
        if not a or not a.domains:
            return None
        return a.domains[0].split(":")[-1]

    def owner_activity(self, urn: str) -> Signal:
        """TRUE if any owner is active, FALSE if all known owners are inactive."""
        a = self.graph.get_aspect(urn, OwnershipClass)
        if not a or not a.owners:
            return Signal.UNKNOWN
        seen_status = False
        for o in a.owners:
            if ":corpuser:" in o.owner:
                info = self.graph.get_aspect(o.owner, CorpUserInfoClass)
                if info is not None and info.active is not None:
                    seen_status = True
                    if info.active:
                        return Signal.TRUE
            elif ":corpGroup:" in o.owner:
                return Signal.TRUE  # a team owner counts as active tending
        return Signal.FALSE if seen_status else Signal.UNKNOWN

    # -- lineage / usage / schema -----------------------------------------
    def consumers(self, dataset_urn: str) -> List[str]:
        try:
            related = self.graph.get_related_entities(
                dataset_urn, [_DOWNSTREAM_REL], RelationshipDirection.INCOMING
            )
            return [r.urn for r in related]
        except Exception:
            return []

    def usage(self, dataset_urn: str, limit: int = 30) -> List[DatasetUsageStatisticsClass]:
        try:
            return self.graph.get_timeseries_values(
                dataset_urn, DatasetUsageStatisticsClass, {}, limit=limit
            )
        except Exception:
            return []

    def schema_fields(self, dataset_urn: str) -> List[str]:
        a = self.graph.get_aspect(dataset_urn, SchemaMetadataClass)
        if not a or not a.fields:
            return []
        return [f.fieldPath for f in a.fields]

    def run_records(self, job_urn: str, max_runs: int = 150) -> List[dict]:
        """Return the newest event for each run instance.

        Finished instances expose ``COMPLETE`` with a result and duration;
        an in-flight run remains in the ``STARTED`` state.
        """
        try:
            related = self.graph.get_related_entities(
                job_urn, ["InstanceOf"], RelationshipDirection.INCOMING
            )
            dpi_urns = [r.urn for r in related][:max_runs]
        except Exception:
            return []
        out: List[dict] = []
        for dpi in dpi_urns:
            try:
                ev = self.graph.get_latest_timeseries_value(
                    dpi, DataProcessInstanceRunEventClass, {}
                )
            except Exception:
                ev = None
            if ev is None:
                continue
            result_type = str(getattr(ev.result, "type", "") or "") if ev.result else ""
            out.append(
                {
                    "status": str(ev.status),
                    "result": result_type,
                    "duration_ms": ev.durationMillis,
                    "start_ms": ev.timestampMillis,
                }
            )
        return out
