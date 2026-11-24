"""快照与交付流水线接口：快照取数、同版导出、外部作业分片领取与续领对账。

与通用资源路由并列，路由层只做参数转发，业务判断全部在领域服务内完成。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.geo_export import GeoExportService
from app.services.geo_snapshot import GeoSnapshotService

router = APIRouter(prefix="/api/pipeline", tags=["快照交付流水线"])


class SnapshotCreatePayload(BaseModel):
    analysis_scope: str = "full"
    grid_level: str = "L6"
    time_range: str = "24h"


class ExportPayload(BaseModel):
    export_format: str = "jsonl"
    shard_size: int = 100
    snapshot_id: str | None = None


class ClaimPayload(BaseModel):
    region_name: str
    job_id: str
    shard_size: int = 5
    snapshot_id: str | None = None


class ShardConfirmPayload(BaseModel):
    shard_no: int
    delivered_checksum: str | None = None


@router.post("/snapshots/{batch_key}")
def create_snapshot(batch_key: str, payload: SnapshotCreatePayload) -> dict[str, Any]:
    return GeoSnapshotService().create_snapshot(
        batch_key,
        analysis_scope=payload.analysis_scope,
        grid_level=payload.grid_level,
        time_range=payload.time_range,
    )


@router.get("/snapshots/{batch_key}")
def list_snapshots(batch_key: str) -> dict[str, Any]:
    svc = GeoSnapshotService()
    snapshots = svc.list_snapshots(batch_key)
    return {"batch_key": batch_key, "total": len(snapshots), "items": snapshots}


@router.get("/snapshots/id/{snapshot_id}")
def get_snapshot(snapshot_id: str) -> dict[str, Any]:
    snapshot = GeoSnapshotService().get_snapshot_by_id(snapshot_id)
    if not snapshot:
        raise HTTPException(status_code=404, detail=f"快照 {snapshot_id} 不存在")
    return snapshot


@router.get("/snapshots/id/{snapshot_id}/recovery")
def get_region_recovery(snapshot_id: str) -> dict[str, Any]:
    recovery = GeoSnapshotService().get_region_recovery(snapshot_id)
    if not recovery:
        raise HTTPException(status_code=404, detail=f"快照 {snapshot_id} 不存在")
    return recovery


@router.post("/exports/{batch_key}")
def export_batch(batch_key: str, payload: ExportPayload) -> dict[str, Any]:
    try:
        return GeoExportService().export_batch_results(
            batch_key,
            export_format=payload.export_format,
            shard_size=payload.shard_size,
            snapshot_id=payload.snapshot_id,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/exports/{batch_key}/summary")
def export_summary(batch_key: str, snapshot_id: str | None = None) -> dict[str, Any]:
    return GeoExportService().get_export_summary(batch_key, snapshot_id)


@router.post("/claims/{batch_key}")
def register_claim(batch_key: str, payload: ClaimPayload) -> dict[str, Any]:
    try:
        return GeoExportService().register_region_claim(
            batch_key,
            region_name=payload.region_name,
            job_id=payload.job_id,
            shard_size=payload.shard_size,
            snapshot_id=payload.snapshot_id,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/claims/{claim_id}")
def claim_status(claim_id: str) -> dict[str, Any]:
    status = GeoExportService().get_claim_status(claim_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"领取请求 {claim_id} 不存在")
    return status


@router.get("/claims/{claim_id}/resume")
def resume_claim(claim_id: str) -> dict[str, Any]:
    try:
        return GeoExportService().resume_region_claim(claim_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/claims/{claim_id}/confirm")
def confirm_claim(claim_id: str, payload: ShardConfirmPayload) -> dict[str, Any]:
    svc = GeoExportService()
    try:
        result = svc.confirm_region_shard(
            claim_id, payload.shard_no, payload.delivered_checksum)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result["message"])
    return result
