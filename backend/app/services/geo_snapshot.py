"""分析快照领域服务：对聚合分析结果创建版本化快照。

快照发布时把热力聚合、OD矩阵、区域聚合三份数据集冻结到以快照号命名的独立
目录（manifest 清单 + 同版数据文件），后续结果导出与分片领取一律按快照号读取
冻结副本，而不是实时读取可能已被更正的当前文件，保证交付的是一批可回查的
同版区域明细。
"""
from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 区域明细行中能够定位到原网格的字段：命中任一字段视为保留了完整原网格
_REGION_GRID_FIELDS = ("grid_id", "geohash", "grid_code", "grid_geohash")


class GeoSnapshotService(BaseModuleService):
    MODULE = "geo_snapshot"
    ENTITY = "版本快照"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["快照编号", "分析口径", "网格层级", "时间范围", "结果条数", "生成方式", "快照水位", "生成时间"]
    REQUIRED_FIELDS = ["快照编号", "分析口径", "网格层级"]
    STATUS_ORDER = ["草稿", "待确认", "已发布", "已归档"]
    ACTION_RULES = {"提交确认": "待确认", "发布快照": "已发布", "标记归档": "已归档"}
    NEGATIVE_ACTIONS = []

    # -- 快照数据布局 --

    def _registry_file(self, batch_key: str) -> Path:
        return self._data_dir("snapshots") / f"{batch_key}_snapshot.jsonl"

    def _frozen_dir(self, snapshot_id: str) -> Path:
        return self._data_dir("snapshots") / "frozen" / snapshot_id

    @staticmethod
    def _region_identity(row: dict[str, Any]) -> str:
        """区域明细行的归属区域标识，优先区域名称，缺失时退回区域编号。"""
        return str(row.get("region_name") or row.get("region_id") or "UNKNOWN")

    @classmethod
    def _region_detail_type(cls, row: dict[str, Any]) -> str:
        """判定区域明细行的保留粒度：完整原网格 / 仅区域合计摘要。"""
        return "full_grid" if any(row.get(field) for field in _REGION_GRID_FIELDS) else "summary_only"

    def _build_region_recovery(self, region_data: list[dict[str, Any]]) -> dict[str, Any]:
        """按区域汇总同版区域明细的可恢复范围。

        完整原网格（full_grid）区域可逐网格恢复全部明细行；只保留合计
        （summary_only）的旧区域没有原网格行，只能恢复区域级合计行。
        """
        grouped: dict[str, dict[str, Any]] = {}
        for row in region_data:
            name = self._region_identity(row)
            bucket = grouped.setdefault(name, {
                "region_name": name,
                "detail_type": self._region_detail_type(row),
                "row_count": 0,
                "grid_rows": 0,
                "total_points": 0,
            })
            bucket["row_count"] += 1
            if bucket["detail_type"] == "full_grid":
                bucket["grid_rows"] += 1
            if isinstance(row.get("total_points"), (int, float)):
                bucket["total_points"] += row["total_points"]

        recovery: dict[str, Any] = {}
        for name, info in grouped.items():
            if info["detail_type"] == "full_grid":
                recoverable = info["row_count"]
                note = "保留完整原网格，可逐网格恢复全部区域明细行"
            else:
                recoverable = info["row_count"]
                note = "仅保留区域合计摘要，只能恢复区域级合计行，原网格明细行不可恢复"
            recovery[name] = {**info, "recoverable_rows": recoverable, "note": note}
        return recovery

    def create_snapshot(self, batch_key: str, analysis_scope: str = "full",
                         grid_level: str = "L6", time_range: str = "24h") -> dict[str, Any]:
        """读取热力聚合、OD矩阵和区域聚合的当前数据集，冻结生成同版快照。"""
        heatmap_file = self._data_dir("heatmap") / f"{batch_key}_heatmap.jsonl"
        od_file = self._data_dir("od_matrix") / f"{batch_key}_od.jsonl"
        region_file = self._data_dir("region_aggregates") / f"{batch_key}_region.jsonl"

        heatmap_data = self._read_jsonl(heatmap_file) if heatmap_file.exists() else []
        od_data = self._read_jsonl(od_file) if od_file.exists() else []
        region_data = self._read_jsonl(region_file) if region_file.exists() else []

        snapshot_id = f"SNAP-{batch_key}-{int(time.time())}-{uuid.uuid4().hex[:6]}"
        frozen_dir = self._frozen_dir(snapshot_id)
        frozen_dir.mkdir(parents=True, exist_ok=True)

        # 冻结三份同版数据集副本：此后当前文件被更正也不会影响该快照
        frozen_heatmap = frozen_dir / "heatmap.jsonl"
        frozen_od = frozen_dir / "od_matrix.jsonl"
        frozen_region = frozen_dir / "region.jsonl"
        self._write_jsonl(frozen_heatmap, heatmap_data)
        self._write_jsonl(frozen_od, od_data)
        self._write_jsonl(frozen_region, region_data)

        region_recovery = self._build_region_recovery(region_data)
        region_detail_types = sorted({info["detail_type"] for info in region_recovery.values()})
        total_results = len(heatmap_data) + len(od_data) + len(region_data)

        snapshot = {
            "snapshot_id": snapshot_id,
            "batch_key": batch_key,
            "analysis_scope": analysis_scope,
            "grid_level": grid_level,
            "time_range": time_range,
            "heatmap_cells": len(heatmap_data),
            "od_pairs": len(od_data),
            "region_aggregates": len(region_data),
            "region_detail_types": region_detail_types,
            "total_results": total_results,
            "peak_heatmap_grid": max((h["point_count"] for h in heatmap_data), default=0),
            "total_trips": sum(od.get("trip_count", 0) for od in od_data),
            "generated_at": time.time(),
            "version": "v1.0",
            "frozen_files": {
                "heatmap": str(frozen_heatmap),
                "od_matrix": str(frozen_od),
                "region": str(frozen_region),
            },
            "region_recovery": region_recovery,
        }

        manifest_file = frozen_dir / "manifest.json"
        with open(manifest_file, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2, default=str)

        # 快照登记表仍按批次追加，保留全部历史快照号，可逐号回查
        self._write_jsonl(self._registry_file(batch_key), [snapshot])

        # 创建快照记录
        entry, _ = self.create_entry({
            "快照编号": snapshot_id,
            "分析口径": analysis_scope,
            "网格层级": grid_level,
            "时间范围": time_range,
            "结果条数": str(total_results),
            "生成方式": "auto_aggregate",
            "快照水位": f"热力{len(heatmap_data)}_OD{len(od_data)}_区域{len(region_data)}",
            "生成时间": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time())),
        })
        if entry:
            # 把同版冻结位置挂到记录上，工作台与导出入口都按快照号核对同版数据
            entry["snapshot_id"] = snapshot_id
            entry["batch_key"] = batch_key
            entry["manifest_file"] = str(manifest_file)
            entry["region_detail"] = ",".join(region_detail_types) or "empty"
            self.run_action(entry["id"], "发布快照")

        return {
            "batch_key": batch_key,
            "snapshot_id": snapshot_id,
            "total_results": total_results,
            "heatmap_cells": len(heatmap_data),
            "od_pairs": len(od_data),
            "region_aggregates": len(region_data),
            "manifest_file": str(manifest_file),
            "frozen_dir": str(frozen_dir),
            "region_recovery": region_recovery,
        }

    # -- 快照读取：只认快照号对应的冻结副本 --

    def list_snapshots(self, batch_key: str) -> list[dict[str, Any]]:
        """读取批次下全部已发布快照登记，按发布时间升序。"""
        return self._read_jsonl(self._registry_file(batch_key))

    def get_snapshot(self, batch_key: str) -> dict[str, Any]:
        """读取批次最新一次发布的分析快照登记。"""
        snapshots = self.list_snapshots(batch_key)
        return snapshots[-1] if snapshots else {}

    def get_snapshot_by_id(self, snapshot_id: str) -> dict[str, Any]:
        """按快照号读取冻结清单；不存在返回空字典。"""
        manifest_file = self._frozen_dir(snapshot_id) / "manifest.json"
        if not manifest_file.exists():
            return {}
        with open(manifest_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def get_frozen_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        """按快照号读取同版冻结数据（热力、OD、区域明细）与清单。"""
        manifest = self.get_snapshot_by_id(snapshot_id)
        if not manifest:
            return {}
        frozen_dir = self._frozen_dir(snapshot_id)
        return {
            "manifest": manifest,
            "heatmap": self._read_jsonl(frozen_dir / "heatmap.jsonl"),
            "od_matrix": self._read_jsonl(frozen_dir / "od_matrix.jsonl"),
            "region": self._read_jsonl(frozen_dir / "region.jsonl"),
        }

    def get_region_recovery(self, snapshot_id: str) -> dict[str, Any]:
        """说明该快照的区域明细可回查范围：完整原网格可恢复哪些行、摘要快照只能恢复合计行。"""
        manifest = self.get_snapshot_by_id(snapshot_id)
        if not manifest:
            return {}
        return {
            "snapshot_id": snapshot_id,
            "region_aggregates": manifest.get("region_aggregates", 0),
            "regions": manifest.get("region_recovery", {}),
        }
