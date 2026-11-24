"""快照版本化与区域分片交付测试。

覆盖问题场景：
1. 快照发布后更正同批聚合文件，导出仍按快照号读取同版冻结明细，条数不漂移；
2. 只保留合计的旧快照可恢复范围可说明、可回查；
3. 中断续领只登记未确认分片，已确认交付的行不会重复登记；
4. 新快照发布后，发布前发起、发布后恢复的旧请求仍取旧版区域明细；
   新快照的领取记录独立对账；
5. 已确认首片的请求，后续交付限于剩余分片。
"""
from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def setup_data_dir(monkeypatch):
    tmpdir = Path(tempfile.mkdtemp(prefix="geo_snap_test_"))
    import app.services.base as base_mod
    original_root = base_mod.DATA_ROOT
    base_mod.DATA_ROOT = tmpdir
    for sub in ["heatmap", "od_matrix", "region_aggregates",
                "snapshots", "exports", "checkpoints"]:
        (tmpdir / sub).mkdir(parents=True, exist_ok=True)
    yield tmpdir
    base_mod.DATA_ROOT = original_root
    shutil.rmtree(tmpdir, ignore_errors=True)


def _write_current_datasets(svc, batch_key: str, *, heatmap_n: int, od_n: int,
                            region_rows: list[dict]) -> None:
    """构造当前版本的热力/OD/区域聚合文件（整文件覆盖，模拟更正后的当前行）。"""
    import json

    heatmap_rows = [
        {"cell_id": i + 1, "geohash": f"wx{i:04d}", "point_count": i + 2}
        for i in range(heatmap_n)
    ]
    od_rows = [
        {"od_id": i + 1, "origin": "wx0001", "destination": f"wx{i:04d}", "trip_count": i + 1}
        for i in range(od_n)
    ]
    for sub, name, rows in [
        ("heatmap", f"{batch_key}_heatmap.jsonl", heatmap_rows),
        ("od_matrix", f"{batch_key}_od.jsonl", od_rows),
        ("region_aggregates", f"{batch_key}_region.jsonl", region_rows),
    ]:
        path = svc._data_dir(sub) / name
        with open(path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def _full_grid_region(region_name: str, n: int) -> list[dict]:
    """有完整原网格的区域：每行带 geohash，可逐网格回查。"""
    return [
        {
            "region_id": f"R-{region_name}",
            "region_name": region_name,
            "geohash": f"wx{g:04d}",
            "grid_id": f"GRID-{g:04d}",
            "total_points": 10 + g,
        }
        for g in range(n)
    ]


def _summary_region(region_name: str) -> dict:
    """旧区只保留合计：没有任何原网格字段。"""
    return {
        "region_id": f"R-{region_name}",
        "region_name": region_name,
        "grid_count": 9,
        "total_points": 99,
    }


class TestVersionedSnapshotExport:
    """快照冻结与同版导出。"""

    def test_export_keeps_snapshot_version_after_source_corrected(self, setup_data_dir):
        from app.services.geo_snapshot import GeoSnapshotService
        from app.services.geo_export import GeoExportService

        snap_svc = GeoSnapshotService()
        export_svc = GeoExportService()
        batch_key = "BATCH-V1"

        # 初始聚合：东城区7条完整原网格 + 旧西城区1条合计；热力2、OD1
        region_v1 = _full_grid_region("东城区", 7) + [_summary_region("旧西城区")]
        _write_current_datasets(
            snap_svc, batch_key, heatmap_n=2, od_n=1, region_rows=region_v1)

        snap1 = snap_svc.create_snapshot(batch_key)
        snap1_id = snap1["snapshot_id"]
        assert snap1["total_results"] == 2 + 1 + 8
        assert snap1["region_aggregates"] == 8

        # 发布后更正同批聚合文件：区域明细追加到12行、热力增加到5行
        region_v2 = region_v1 + _full_grid_region("南城区", 4)
        _write_current_datasets(
            snap_svc, batch_key, heatmap_n=5, od_n=1, region_rows=region_v2)

        # 同一个快照号重新交付：必须仍读冻结副本，不能读当前文件（当前共18行）
        result = export_svc.export_batch_results(
            batch_key, shard_size=4, snapshot_id=snap1_id)
        assert result["snapshot_id"] == snap1_id
        assert result["exported_records"] == 11  # 2+1+8，而不是当前的 5+1+12
        assert result["shards"] == 3

        # 分片文件落在快照号专属目录，逐片内容与冻结同版数据一致
        frozen = snap_svc.get_frozen_snapshot(snap1_id)
        expected_rows = frozen["heatmap"] + frozen["od_matrix"] + frozen["region"]
        exported_rows = []
        for path in result["shard_files"]:
            exported_rows.extend(export_svc._read_jsonl(Path(path)))
        assert exported_rows == expected_rows

        # 汇总同样按快照号记录条数，重新交付不串档
        summary = export_svc.get_export_summary(batch_key, snap1_id)
        assert summary["total_records"] == 11
        assert summary["snapshot_id"] == snap1_id

        # 不显式传快照号时取批次最新快照；发布新快照后不会错读成旧号
        snap2 = snap_svc.create_snapshot(batch_key)
        latest_export = export_svc.export_batch_results(batch_key, shard_size=100)
        assert latest_export["snapshot_id"] == snap2["snapshot_id"]
        assert latest_export["exported_records"] == 5 + 1 + 12

        # 快照登记表保留两个版本，同号可回查，旧号不再指向当前文件
        registry = snap_svc.list_snapshots(batch_key)
        assert [s["snapshot_id"] for s in registry] == [snap1_id, snap2["snapshot_id"]]
        assert snap_svc.get_snapshot(batch_key)["snapshot_id"] == snap2["snapshot_id"]
        assert snap_svc.get_snapshot_by_id(snap1_id)["region_aggregates"] == 8

    def test_region_recovery_distinguishes_full_grid_and_summary(self, setup_data_dir):
        from app.services.geo_snapshot import GeoSnapshotService

        snap_svc = GeoSnapshotService()
        batch_key = "BATCH-REC"
        region_rows = _full_grid_region("东城区", 7) + [_summary_region("旧西城区")]
        _write_current_datasets(
            snap_svc, batch_key, heatmap_n=1, od_n=1, region_rows=region_rows)

        snap = snap_svc.create_snapshot(batch_key)
        recovery = snap_svc.get_region_recovery(snap["snapshot_id"])

        assert recovery["region_aggregates"] == 8
        dongcheng = recovery["regions"]["东城区"]
        assert dongcheng["detail_type"] == "full_grid"
        assert dongcheng["row_count"] == 7
        assert dongcheng["recoverable_rows"] == 7
        assert "逐网格恢复" in dongcheng["note"]

        old_region = recovery["regions"]["旧西城区"]
        assert old_region["detail_type"] == "summary_only"
        assert old_region["row_count"] == 1
        assert old_region["recoverable_rows"] == 1  # 只能恢复合计行本身
        assert "原网格明细行不可恢复" in old_region["note"]

    def test_unknown_snapshot_id_rejected(self, setup_data_dir):
        from app.services.geo_export import GeoExportService

        with pytest.raises(FileNotFoundError):
            GeoExportService().export_batch_results("BATCH-X", snapshot_id="SNAP-NOPE")


class TestResumableRegionClaims:
    """外部作业分片领取、中断续领与跨快照对账。"""

    def _publish_two_snapshots(self):
        from app.services.geo_snapshot import GeoSnapshotService

        snap_svc = GeoSnapshotService()
        batch_key = "BATCH-CLAIM"
        region_v1 = _full_grid_region("东城区", 7) + [_summary_region("旧西城区")]
        _write_current_datasets(
            snap_svc, batch_key, heatmap_n=2, od_n=1, region_rows=region_v1)
        snap1 = snap_svc.create_snapshot(batch_key)

        # 新聚合：当前文件被更正（新增南城区4行）
        region_v2 = region_v1 + _full_grid_region("南城区", 4)
        _write_current_datasets(
            snap_svc, batch_key, heatmap_n=3, od_n=1, region_rows=region_v2)
        snap2 = snap_svc.create_snapshot(batch_key)
        return snap_svc, batch_key, snap1["snapshot_id"], snap2["snapshot_id"]

    def test_resume_only_returns_unconfirmed_shards_and_no_duplicate_registration(self):
        from app.services.geo_export import GeoExportService
        from app.store import store

        export_svc = GeoExportService()
        _, batch_key, snap1_id, snap2_id = self._publish_two_snapshots()

        # 外部作业为东城区登记领取，7行/每片3行 → 3个分片(3,3,1)
        claim = export_svc.register_region_claim(
            batch_key, "东城区", job_id="JOB-1", shard_size=3, snapshot_id=snap1_id)
        claim_id = claim["claim_id"]
        assert claim["snapshot_id"] == snap1_id
        assert [s["row_count"] for s in claim["shards"]] == [3, 3, 1]
        entry_id = claim["entry_id"]

        # 确认首片交付
        first_checksum = claim["shards"][0]["checksum"]
        confirm = export_svc.confirm_region_shard(claim_id, 1, first_checksum)
        assert confirm["ok"] is True
        assert confirm["confirmed_shards"] == 1

        # 中断后重新登记同一请求：幂等返回，不产生新条目、不重置确认进度
        again = export_svc.register_region_claim(
            batch_key, "东城区", job_id="JOB-1", shard_size=3, snapshot_id=snap1_id)
        assert again["idempotent_reused"] is True
        assert again["claim_id"] == claim_id
        claim_entries = [
            r for r in store.rows("geo_export") if r.get("claim_id") == claim_id]
        assert len(claim_entries) == 1
        assert claim_entries[0]["id"] == entry_id

        # 重新打开快照仍能找回已确认数据；续领只给未确认分片
        status = export_svc.get_claim_status(claim_id)
        assert status["confirmed_shard_nos"] == [1]
        assert status["pending_shard_nos"] == [2, 3]
        assert status["confirmed_row_count"] == 3

        resume = export_svc.resume_region_claim(claim_id)
        assert [s["shard_no"] for s in resume["pending_shards"]] == [2, 3]
        assert resume["confirmed_row_count"] == 3
        assert resume["pending_row_count"] == 4

        # 已确认首片再次确认：幂等，不会重复登记已交付的行
        repeat = export_svc.confirm_region_shard(claim_id, 1, first_checksum)
        assert repeat["ok"] is True
        assert repeat["already_confirmed"] is True
        status = export_svc.get_claim_status(claim_id)
        assert status["confirmed_row_count"] == 3  # 行数没有翻倍

        # 错误版本（当前文件/其他快照）的分片校验值不一致，拒绝确认
        bad = export_svc.confirm_region_shard(claim_id, 2, "wrong-checksum")
        assert bad["ok"] is False
        assert export_svc.get_claim_status(claim_id)["pending_shard_nos"] == [2, 3]

        # 交付剩余分片后，工作台条目流转为已交付
        shard2 = next(s for s in resume["pending_shards"] if s["shard_no"] == 2)
        shard3 = next(s for s in resume["pending_shards"] if s["shard_no"] == 3)
        export_svc.confirm_region_shard(claim_id, 2, shard2["checksum"])
        final = export_svc.confirm_region_shard(claim_id, 3, shard3["checksum"])
        assert final["confirmed_shards"] == 3
        entry = store.find("geo_export", entry_id)
        assert entry["status"] == "已交付"

    def test_claim_started_before_publish_reads_old_snapshot_after_new_publish(self):
        """发布前发起、一行未确认的请求，发布后续领仍取旧快照同版区域明细。"""
        from app.services.geo_export import GeoExportService

        export_svc = GeoExportService()
        _, batch_key, snap1_id, snap2_id = self._publish_two_snapshots()

        # 旧西城区在旧快照里只有1条合计行；请求在新快照发布前已登记，零确认
        claim = export_svc.register_region_claim(
            batch_key, "旧西城区", job_id="JOB-2", shard_size=3, snapshot_id=snap1_id)
        claim_id = claim["claim_id"]
        assert claim["region_row_count"] == 1
        assert claim["shards"][0]["detail_type"] == "summary_only"

        # 新快照已发布（外部环境发生变化），恢复旧请求
        resume = export_svc.resume_region_claim(claim_id)
        assert resume["snapshot_id"] == snap1_id          # 实际取得的是旧版区域明细
        assert len(resume["pending_shards"]) == 1
        assert resume["pending_row_count"] == 1           # 仍是旧快照的1条合计行
        assert resume["confirmed_shards"] == 0

        # 新快照里根本没有以“旧西城区”名义新增的行，独立领取互不影响
        from app.services.geo_snapshot import GeoSnapshotService
        snap2_region = GeoSnapshotService().get_frozen_snapshot(snap2_id)["region"]
        old_rows_v2 = [r for r in snap2_region if r.get("region_name") == "旧西城区"]
        assert len(old_rows_v2) == 1

    def test_new_snapshot_claim_reconciles_independently(self):
        from app.services.geo_export import GeoExportService

        export_svc = GeoExportService()
        _, batch_key, snap1_id, snap2_id = self._publish_two_snapshots()

        old_claim = export_svc.register_region_claim(
            batch_key, "东城区", job_id="JOB-3", shard_size=3, snapshot_id=snap1_id)
        export_svc.confirm_region_shard(
            old_claim["claim_id"], 1, old_claim["shards"][0]["checksum"])

        # 新快照发布后同区同作业重新领取：新记录、新快照号、独立分片与对账
        new_claim = export_svc.register_region_claim(
            batch_key, "东城区", job_id="JOB-3", shard_size=3, snapshot_id=snap2_id)
        assert new_claim["claim_id"] != old_claim["claim_id"]
        assert new_claim["snapshot_id"] == snap2_id
        assert all(not s["confirmed"] for s in new_claim["shards"])

        new_status = export_svc.get_claim_status(new_claim["claim_id"])
        assert new_status["confirmed_row_count"] == 0
        assert new_status["pending_row_count"] == 7

        # 旧请求的确认进度原样保留，仍绑定旧快照
        old_status = export_svc.get_claim_status(old_claim["claim_id"])
        assert old_status["snapshot_id"] == snap1_id
        assert old_status["confirmed_shard_nos"] == [1]

    def test_delivery_after_first_shard_limited_to_remaining_shards(self):
        from app.services.geo_export import GeoExportService

        export_svc = GeoExportService()
        _, batch_key, snap1_id, _ = self._publish_two_snapshots()

        claim = export_svc.register_region_claim(
            batch_key, "东城区", job_id="JOB-4", shard_size=2, snapshot_id=snap1_id)
        claim_id = claim["claim_id"]
        assert [s["row_count"] for s in claim["shards"]] == [2, 2, 2, 1]

        # 确认首片
        export_svc.confirm_region_shard(
            claim_id, 1, claim["shards"][0]["checksum"])

        # 后续交付只能领取剩余分片（2/3/4），首片不再出现在续领清单
        resume = export_svc.resume_region_claim(claim_id)
        assert [s["shard_no"] for s in resume["pending_shards"]] == [2, 3, 4]
        assert resume["pending_row_count"] == 5
        assert resume["confirmed_row_count"] == 2

        for shard in resume["pending_shards"]:
            result = export_svc.confirm_region_shard(
                claim_id, shard["shard_no"], shard["checksum"])
            assert result["ok"] is True
        assert export_svc.get_claim_status(claim_id)["complete"] is True
