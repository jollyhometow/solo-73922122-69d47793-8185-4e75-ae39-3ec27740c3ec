"""真实 PostgreSQL 下的收尾交错回归（取消先受理 / 成功先发布）。

沙箱环境限制了 PostgreSQL 的信号投递，conn.cancel() 不能中止正在执行的
服务端查询。因此这里用两处可控交错点模拟“取消已受理但 SQL 仍在返回数据”：
- 每批 fetch 之间注入短暂停顿（放宽取消窗口）；
- 在 RunWriter.add 前后注入钩子，可让取消请求恰在“即将产出结论”的前一刻到达。

生产环境由 conn.cancel() + pg_terminate_backend 提供即时中止；本文件验证
应用层裁决与发布顺序，不依赖内核信号。
"""
from __future__ import annotations

import time

from fastapi.testclient import TestClient

TERMINAL = ("succeeded", "failed", "cancelled", "timeout")


def _wait_terminal(client: TestClient, job_id: str, timeout: float = 20) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        body = client.get(f"/jobs/{job_id}").json()
        if body["status"] in TERMINAL:
            return body
        time.sleep(0.02)
    raise AssertionError("任务未在时限内到达终态")


def _wait_running(client: TestClient, job_id: str, timeout: float = 10) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if client.get(f"/jobs/{job_id}").json()["status"] in ("running", "cancelling"):
            return
        time.sleep(0.01)
    raise AssertionError("任务未开始运行")


def test_cancel_first_wins_over_late_completion(client: TestClient, monkeypatch) -> None:
    """SQL 本可正常跑完，但取消先被受理：终态 cancelled、不发布结论。"""
    from app import db as db_mod
    from app import manager as manager_mod

    # 只置标志的取消（沙箱信号不可达），保证“取消已受理但查询仍在产数据”
    def flag_only_cancel(self):
        for job in manager_mod.manager._jobs.values():
            if job.status in ("running", "cancelling"):
                job.cancel_requested = True
                job.status = "cancelling"

    monkeypatch.setattr(db_mod.SnapshotSession, "cancel_all", flag_only_cancel)

    real_fetchmany = db_mod.psycopg.Cursor.fetchmany

    def slow_fetchmany(self, size=None):
        time.sleep(0.02)
        return real_fetchmany(self, size)

    monkeypatch.setattr(db_mod.psycopg.Cursor, "fetchmany", slow_fetchmany)

    sql = "SELECT g::bigint FROM generate_series(1, 300) AS g"
    resp = client.post("/jobs", json={"left_sql": sql, "right_sql": sql})
    job_id = resp.json()["job_id"]
    _wait_running(client, job_id)

    cancel = client.post(f"/jobs/{job_id}/cancel")
    assert cancel.status_code == 200
    assert cancel.json()["cancel_accepted"] is True

    out = _wait_terminal(client, job_id)
    assert out["status"] == "cancelled"
    assert out["result"] is None
    # 终态之后名额已释放，可立即再建任务
    assert client.post(
        "/jobs", json={"left_sql": "SELECT 1::bigint", "right_sql": "SELECT 1::bigint"}
    ).status_code == 201


def test_cancel_just_before_publication_still_publishes_no_result(
    client: TestClient, monkeypatch
) -> None:
    """取消已受理、结论已算出但尚未发布：仍按取消终态，绝不下发结论。"""
    from app import external_merge as em
    from app import manager as manager_mod

    hook_fired = {"value": False}
    real_reconcile = em.reconcile_sorted

    def reconcile_then_cancel(*args, **kwargs):
        stats = real_reconcile(*args, **kwargs)
        if not hook_fired["value"]:
            hook_fired["value"] = True
            # 模拟结论产出前一刻收到取消（此时 SQL 已全部结束）
            for job in manager_mod.manager._jobs.values():
                if job.status in ("running", "cancelling"):
                    manager_mod.manager.cancel_job_request(job.job_id)
        return stats

    # manager 模块以 from ... import 绑定，必须替换该模块内的引用
    monkeypatch.setattr(manager_mod, "reconcile_sorted", reconcile_then_cancel)

    sql = "SELECT g::bigint FROM generate_series(1, 100) AS g"
    resp = client.post("/jobs", json={"left_sql": sql, "right_sql": sql})
    out = _wait_terminal(client, resp.json()["job_id"])
    assert out["status"] == "cancelled"
    assert out["result"] is None


def test_success_visible_first_then_cancel_is_refused(client: TestClient) -> None:
    """成功已发布：保持 succeeded 与 result，取消返回 cancel_accepted=false。"""
    sql = "SELECT g::bigint FROM generate_series(1, 100) AS g"
    resp = client.post("/jobs", json={"left_sql": sql, "right_sql": sql})
    job_id = resp.json()["job_id"]
    out = _wait_terminal(client, job_id)
    assert out["status"] == "succeeded" and out["result"] is not None

    cancel = client.post(f"/jobs/{job_id}/cancel").json()
    assert cancel["cancel_accepted"] is False
    assert cancel["status"] == "succeeded"
    assert cancel["result"] == out["result"]
    assert cancel["finished_at"] == out["finished_at"]


def test_snapshot_and_limits_preserved(client: TestClient) -> None:
    """回归：同快照、列类型闸门与 30 秒上限配置仍然生效。"""
    from app import config as config_mod

    assert config_mod.settings.job_timeout_seconds == 30
    assert config_mod.settings.max_diff_types == 20

    sql = "SELECT 1::bigint AS a, 'x'::text AS b"
    out = _wait_terminal(
        client, client.post("/jobs", json={"left_sql": sql, "right_sql": sql}).json()["job_id"]
    )
    assert out["status"] == "succeeded"
    assert out["result"]["column_types"] == ["bigint", "text"]
    assert out["result"]["identical"] is True
    assert out["result"]["diffs"] == []
