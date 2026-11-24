"""结果导出领域服务：按快照号交付同版冻结数据，并支持外部作业分片领取与中断续领。

关键约束：
- 导出与分片领取只读取快照发布时冻结的同版副本（按快照号定位），不再实时读取
  当前热力/OD/区域聚合文件，避免快照发布后聚合文件被更正导致下载条数与快照对不上。
- 领取登记按快照号+作业+区域幂等；续领只返回尚未确认的分片，已确认交付的行不会
  被再次登记。新快照产生的领取记录使用新的快照号，独立对账，互不串档。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.geo_snapshot import GeoSnapshotService
from app.store import store


class GeoExportService(BaseModuleService):
    MODULE = "geo_export"
    ENTITY = "导出任务"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["导出编号", "快照编号", "导出格式", "区域范围", "字段范围", "记录条数", "分片大小", "导出水位"]
    REQUIRED_FIELDS = ["导出编号", "快照编号", "导出格式"]
    STATUS_ORDER = ["草稿", "待导出", "导出中", "已交付"]
    ACTION_RULES = {"启动导出": "导出中", "确认交付": "已交付", "标记失败": "已交付"}
    NEGATIVE_ACTIONS = []

    def _snapshot_service(self) -> GeoSnapshotService:
        return GeoSnapshotService()

    def _resolve_frozen(self, batch_key: str, snapshot_id: str | None) -> dict[str, Any]:
        """定位要交付的同版冻结数据；显式快照号优先，否则取批次最新快照。

        批次尚未发布过快照时（兼容旧入口），先按当前文件冻结一份再交付，
        保证任何一次导出都对应一批可回查的同版数据。
        """
        snap_svc = self._snapshot_service()
        if not snapshot_id:
            latest = snap_svc.get_snapshot(batch_key)
            if latest:
                snapshot_id = latest["snapshot_id"]
            else:
                created = snap_svc.create_snapshot(batch_key)
                snapshot_id = created["snapshot_id"]
        frozen = snap_svc.get_frozen_snapshot(snapshot_id)
        if not frozen:
            raise FileNotFoundError(f"快照 {snapshot_id} 的冻结数据不存在，无法按同版数据交付")
        return frozen

    def _export_dir(self, snapshot_id: str) -> "Any":
        return self._data_dir("exports") / snapshot_id

    def export_batch_results(self, batch_key: str, export_format: str = "jsonl",
                              shard_size: int = 100,
                              snapshot_id: str | None = None) -> dict[str, Any]:
        """按快照号读取同版冻结数据，将热力、OD矩阵和区域数据分片导出。"""
        frozen = self._resolve_frozen(batch_key, snapshot_id)
        snapshot_id = frozen["manifest"]["snapshot_id"]

        results = []
        results.extend(frozen["heatmap"])
        results.extend(frozen["od_matrix"])
        results.extend(frozen["region"])
        total_records = len(results)

        if not results:
            return {
                "batch_key": batch_key,
                "snapshot_id": snapshot_id,
                "exported": 0,
                "total": 0,
                "shards": [],
            }

        export_dir = self._export_dir(snapshot_id)
        export_dir.mkdir(parents=True, exist_ok=True)

        shards = []
        for i in range(0, len(results), shard_size):
            shard = results[i:i + shard_size]
            shard_no = i // shard_size + 1
            shard_file = export_dir / f"shard_{shard_no:03d}.{export_format}"
            self._write_jsonl(shard_file, shard)
            shards.append(str(shard_file))

        export_summary = {
            "batch_key": batch_key,
            "snapshot_id": snapshot_id,
            "export_format": export_format,
            "total_records": total_records,
            "shards": len(shards),
            "shard_size": shard_size,
            "exported_at": time.time(),
            "shard_files": shards,
        }
        # 汇总按快照号独立留存，重复交付不会把不同版本的条数混写
        summary_file = export_dir / "export_summary.jsonl"
        self._write_jsonl(summary_file, [export_summary])

        entry, _ = self.create_entry({
            "导出编号": f"EXPORT-{snapshot_id}",
            "快照编号": snapshot_id,
            "导出格式": export_format,
            "区域范围": "all",
            "字段范围": "all",
            "记录条数": str(total_records),
            "分片大小": str(shard_size),
            "导出水位": f"已导出{total_records}条/{len(shards)}分片",
        })
        if entry:
            entry["snapshot_id"] = snapshot_id
            entry["batch_key"] = batch_key
            self.run_action(entry["id"], "确认交付")

        return {
            "batch_key": batch_key,
            "snapshot_id": snapshot_id,
            "exported_records": total_records,
            "shards": len(shards),
            "format": export_format,
            "shard_files": shards,
        }

    def get_export_summary(self, batch_key: str, snapshot_id: str | None = None) -> dict[str, Any]:
        """读取批次（或指定快照号）最近一次导出汇总。"""
        if not snapshot_id:
            latest = self._snapshot_service().get_snapshot(batch_key)
            if not latest:
                return {}
            snapshot_id = latest["snapshot_id"]
        summaries = self._read_jsonl(self._export_dir(snapshot_id) / "export_summary.jsonl")
        return summaries[-1] if summaries else {}

    # -- 外部作业分片领取 / 确认 / 中断续领 --

    def _claim_checkpoint(self, claim_id: str):
        return self._data_dir("checkpoints") / f"export_claim_{claim_id}.json"

    @staticmethod
    def _make_claim_id(snapshot_id: str, job_id: str, region_name: str) -> str:
        return f"{snapshot_id}|{job_id}|{region_name}"

    def _build_region_shards(self, region_rows: list[dict[str, Any]], region_name: str,
                             shard_size: int) -> list[dict[str, Any]]:
        """按区域过滤同版区域明细并切分，逐分片固化条数与校验值用于对账。"""
        region_rows = [
            row for row in region_rows
            if GeoSnapshotService._region_identity(row) == region_name
        ]
        shards = []
        for i in range(0, len(region_rows), shard_size):
            chunk = region_rows[i:i + shard_size]
            shards.append({
                "shard_no": i // shard_size + 1,
                "row_count": len(chunk),
                "checksum": self._compute_checksum(
                    "\n".join(str(sorted(row.items(), key=lambda kv: str(kv[0]))) for row in chunk)
                ),
                "detail_type": GeoSnapshotService._region_detail_type(chunk[0]) if chunk else "empty",
                "confirmed": False,
                "confirmed_at": None,
            })
        return shards

    def register_region_claim(self, batch_key: str, region_name: str, job_id: str,
                              shard_size: int = 5,
                              snapshot_id: str | None = None) -> dict[str, Any]:
        """外部作业为某区域登记领取请求（发布前/中断后均可重复调用，幂等）。

        重复登记同一快照号+作业+区域时直接返回原请求，不会重新登记已经确认
        交付的分片；指定不同快照号则生成相互独立的新领取记录。
        """
        frozen = self._resolve_frozen(batch_key, snapshot_id)
        snapshot_id = frozen["manifest"]["snapshot_id"]
        claim_id = self._make_claim_id(snapshot_id, job_id, region_name)

        existing = self._read_checkpoint(f"export_claim_{claim_id}")
        if existing is not None:
            existing["idempotent_reused"] = True
            return existing

        shards = self._build_region_shards(frozen["region"], region_name, shard_size)
        state = {
            "claim_id": claim_id,
            "job_id": job_id,
            "batch_key": batch_key,
            "snapshot_id": snapshot_id,
            "region_name": region_name,
            "shard_size": shard_size,
            "region_row_count": sum(s["row_count"] for s in shards),
            "shards": shards,
            "created_at": time.time(),
            "idempotent_reused": False,
        }
        self._write_checkpoint(f"export_claim_{claim_id}", state)

        # 工作台同步留痕，状态随确认进度流转
        entry, _ = self.create_entry({
            "导出编号": f"CLAIM-{job_id}-{region_name}",
            "快照编号": snapshot_id,
            "导出格式": "jsonl",
            "区域范围": region_name,
            "字段范围": "region_detail",
            "记录条数": str(state["region_row_count"]),
            "分片大小": str(shard_size),
            "导出水位": f"已领取0/{len(shards)}分片",
        })
        if entry:
            entry["claim_id"] = claim_id
            entry["snapshot_id"] = snapshot_id
            entry["batch_key"] = batch_key
            state["entry_id"] = entry["id"]
            self._write_checkpoint(f"export_claim_{claim_id}", state)
            self.run_action(entry["id"], "启动导出")

        return state

    def resume_region_claim(self, claim_id: str) -> dict[str, Any]:
        """中断后续领：只返回该请求尚未确认的分片，已确认分片不再登记。"""
        state = self._read_checkpoint(f"export_claim_{claim_id}")
        if state is None:
            raise FileNotFoundError(f"领取请求 {claim_id} 不存在")
        pending = [dict(shard) for shard in state["shards"] if not shard["confirmed"]]
        confirmed = [dict(shard) for shard in state["shards"] if shard["confirmed"]]
        return {
            "claim_id": claim_id,
            "snapshot_id": state["snapshot_id"],
            "region_name": state["region_name"],
            "total_shards": len(state["shards"]),
            "confirmed_shards": len(confirmed),
            "pending_shards": pending,
            "confirmed_row_count": sum(s["row_count"] for s in confirmed),
            "pending_row_count": sum(s["row_count"] for s in pending),
        }

    def confirm_region_shard(self, claim_id: str, shard_no: int,
                             delivered_checksum: str | None = None) -> dict[str, Any]:
        """确认某分片已交付；重复确认幂等，已确认行不会被重复登记。

        校验值可选：传入时必须与领取时按同版冻结明细固化的校验值一致，
        防止把其他版本/当前文件的行当作该快照分片交付。
        """
        state = self._read_checkpoint(f"export_claim_{claim_id}")
        if state is None:
            raise FileNotFoundError(f"领取请求 {claim_id} 不存在")
        target = next((s for s in state["shards"] if s["shard_no"] == shard_no), None)
        if target is None:
            raise ValueError(f"领取请求 {claim_id} 不存在分片 {shard_no}")

        if delivered_checksum is not None and delivered_checksum != target["checksum"]:
            return {
                "ok": False,
                "claim_id": claim_id,
                "shard_no": shard_no,
                "message": "分片校验值与快照同版区域明细不一致，拒绝确认",
                "expected_checksum": target["checksum"],
            }

        already = target["confirmed"]
        if not already:
            target["confirmed"] = True
            target["confirmed_at"] = time.time()
            self._write_checkpoint(f"export_claim_{claim_id}", state)

        confirmed_count = sum(1 for s in state["shards"] if s["confirmed"])
        entry_id = state.get("entry_id")
        if entry_id is not None:
            entry = store.find(self.MODULE, entry_id)
            if entry is not None:
                confirmed_rows = sum(s["row_count"] for s in state["shards"] if s["confirmed"])
                entry["导出水位"] = f"已领取{confirmed_count}/{len(state['shards'])}分片"
                if confirmed_count == len(state["shards"]):
                    self.run_action(entry_id, "确认交付")

        return {
            "ok": True,
            "claim_id": claim_id,
            "snapshot_id": state["snapshot_id"],
            "region_name": state["region_name"],
            "shard_no": shard_no,
            "already_confirmed": already,
            "row_count": target["row_count"],
            "confirmed_shards": confirmed_count,
            "total_shards": len(state["shards"]),
        }

    def get_claim_status(self, claim_id: str) -> dict[str, Any]:
        """读取领取请求的完整对账状态（快照号、分片确认进度、行级水位）。"""
        state = self._read_checkpoint(f"export_claim_{claim_id}")
        if state is None:
            return {}
        confirmed_shards = [s for s in state["shards"] if s["confirmed"]]
        pending_shards = [s for s in state["shards"] if not s["confirmed"]]
        return {
            "claim_id": claim_id,
            "job_id": state["job_id"],
            "batch_key": state["batch_key"],
            "snapshot_id": state["snapshot_id"],
            "region_name": state["region_name"],
            "total_shards": len(state["shards"]),
            "confirmed_shard_nos": [s["shard_no"] for s in confirmed_shards],
            "pending_shard_nos": [s["shard_no"] for s in pending_shards],
            "confirmed_row_count": sum(s["row_count"] for s in confirmed_shards),
            "pending_row_count": sum(s["row_count"] for s in pending_shards),
            "region_row_count": state["region_row_count"],
            "complete": not pending_shards,
        }
