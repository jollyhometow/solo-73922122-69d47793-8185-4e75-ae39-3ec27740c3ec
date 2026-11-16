"""同一已提交快照隔离测试：核对进行中改写源表，本轮结果不变。"""
from __future__ import annotations

import time

import psycopg
from fastapi.testclient import TestClient


def test_snapshot_isolation_during_running_job(client: TestClient, pg_dsn: str) -> None:
    # 一个会执行数秒的大查询，期间在另一连接里更正源表
    slow_left = (
        "SELECT meter_id::bigint, (wh + gs)::bigint AS wh "
        "FROM readings, generate_series(1, 200000) AS gs "
        "WHERE meter_id = 1001"
    )
    slow_right = slow_left

    resp = client.post("/jobs", json={"left_sql": slow_left, "right_sql": slow_right})
    assert resp.status_code == 201
    job_id = resp.json()["job_id"]

    # 等任务进入 running，随后在独立连接里更正源表并提交
    deadline = time.time() + 10
    while time.time() < deadline:
        body = client.get(f"/jobs/{job_id}").json()
        if body["status"] == "running":
            break
        time.sleep(0.05)

    writer = psycopg.connect(pg_dsn, autocommit=True)
    try:
        # 先记录原值，测试结束恢复，避免污染其他用例的共享数据
        old = writer.execute(
            "SELECT wh FROM readings WHERE meter_id = 1001 "
            "AND read_time = TIMESTAMPTZ '2026-11-15 08:00:00+00'"
        ).fetchone()
        old_wh = old[0] if old else None
        writer.execute(
            "UPDATE readings SET wh = wh + 7 WHERE meter_id = 1001 AND read_time = "
            "TIMESTAMPTZ '2026-11-15 08:00:00+00'"
        )
        writer.execute(
            "INSERT INTO readings VALUES (1001, TIMESTAMPTZ '2026-11-15 11:00:00+00', 9999) "
            "ON CONFLICT DO NOTHING"
        )

        out = _wait(client, job_id)
        # 快照在事务开始时固定，源表的已提交更正对本轮不可见 -> 两侧仍然一致
        assert out["status"] == "succeeded"
        assert out["result"]["identical"] is True
        assert out["result"]["diff_group_total"] == 0
    finally:
        writer.execute(
            "UPDATE readings SET wh = %s WHERE meter_id = 1001 AND read_time = "
            "TIMESTAMPTZ '2026-11-15 08:00:00+00'",
            (old_wh,),
        )
        writer.execute(
            "DELETE FROM readings WHERE meter_id = 1001 "
            "AND read_time = TIMESTAMPTZ '2026-11-15 11:00:00+00'"
        )
        writer.close()


def _wait(client: TestClient, job_id: str) -> dict:
    for _ in range(400):
        body = client.get(f"/jobs/{job_id}").json()
        if body["status"] in ("succeeded", "failed", "cancelled", "timeout"):
            return body
        time.sleep(0.1)
    raise AssertionError("任务未结束")
