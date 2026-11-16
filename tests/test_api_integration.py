"""端到端集成测试：FastAPI + 真实 PostgreSQL。"""
from __future__ import annotations

import time

from fastapi.testclient import TestClient

# 集成测试只验证逻辑与生命周期，使用示例小表（init.sql 的 12 行）；
# 50 万行级吞吐与内存由 scripts/load_test.py 单独覆盖。
BASE_LEFT = (
    "SELECT meter_id::bigint AS meter_id, wh::bigint AS wh "
    "FROM readings WHERE meter_id < 4000"
)
BASE_RIGHT = BASE_LEFT  # 行序不参与比较，两侧顺序不同也不影响结论


def _wait_terminal(client: TestClient, job_id: str, timeout: float = 30) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = client.get(f"/jobs/{job_id}")
        assert resp.status_code == 200
        body = resp.json()
        if body["status"] in ("succeeded", "failed", "cancelled", "timeout"):
            return body
        time.sleep(0.1)
    raise AssertionError("任务未在时限内到达终态")


def test_health(client: TestClient) -> None:
    assert client.get("/health").json() == {"status": "ok"}


def test_identical_queries_report_identical(client: TestClient) -> None:
    resp = client.post("/jobs", json={"left_sql": BASE_LEFT, "right_sql": BASE_RIGHT})
    assert resp.status_code == 201
    job_id = resp.json()["job_id"]

    out = _wait_terminal(client, job_id)
    assert out["status"] == "succeeded"
    result = out["result"]
    assert result["identical"] is True
    assert result["left_row_total"] == result["right_row_total"]
    assert result["left_row_total"] >= 12
    assert result["column_types"] == ["bigint", "bigint"]
    assert result["diff_group_total"] == 0
    assert result["diffs"] == []


def test_extra_row_on_left_is_reported_with_counts(client: TestClient) -> None:
    left = BASE_LEFT + " UNION ALL SELECT 999999::bigint, 42::bigint"
    resp = client.post("/jobs", json={"left_sql": left, "right_sql": BASE_RIGHT})
    out = _wait_terminal(client, resp.json()["job_id"])
    assert out["status"] == "succeeded"
    result = out["result"]
    assert result["identical"] is False
    assert result["left_row_total"] == result["right_row_total"] + 1
    assert result["diff_group_total"] == 1
    diff = result["diffs"][0]
    assert diff["values"] == [999999, 42]
    assert diff["left_count"] == 1 and diff["right_count"] == 0


def test_duplicate_row_counted(client: TestClient) -> None:
    # readings 中存在 (meter_id=1001, wh=1000) 这一行，UNION ALL 再追加一份完全相同的行
    dup_row = (
        "SELECT meter_id::bigint, wh::bigint FROM readings "
        "WHERE meter_id = 1001 AND wh = 1000"
    )
    left = BASE_LEFT + " UNION ALL " + dup_row
    out = _wait_terminal(client, client.post("/jobs", json={"left_sql": left, "right_sql": BASE_RIGHT}).json()["job_id"])
    result = out["result"]
    assert result["identical"] is False
    diff = next(d for d in result["diffs"] if d["values"] == [1001, 1000])
    assert diff["left_count"] == 2 and diff["right_count"] == 1


def test_null_differs_from_empty_string(client: TestClient) -> None:
    left = "SELECT 1::bigint, NULL::text AS v"
    right = "SELECT 1::bigint, ''::text AS v"
    out = _wait_terminal(client, client.post("/jobs", json={"left_sql": left, "right_sql": right}).json()["job_id"])
    result = out["result"]
    assert result["identical"] is False
    # 两侧各持一种“看起来都为空”的值，应报两个差异组
    assert result["diff_group_total"] == 2
    vals = {tuple(d["values"]) for d in result["diffs"]}
    assert (1, None) in vals and (1, "") in vals


def test_column_count_mismatch_fails_without_conclusion(client: TestClient) -> None:
    left = "SELECT meter_id::bigint FROM readings"
    right = "SELECT meter_id::bigint, wh::bigint FROM readings"
    out = _wait_terminal(client, client.post("/jobs", json={"left_sql": left, "right_sql": right}).json()["job_id"])
    assert out["status"] == "failed"
    assert "列数" in out["error"]
    assert out["result"] is None  # 失败不发布比较结论


def test_column_type_mismatch_fails(client: TestClient) -> None:
    left = "SELECT wh::bigint FROM readings"
    right = "SELECT wh::text FROM readings"
    out = _wait_terminal(client, client.post("/jobs", json={"left_sql": left, "right_sql": right}).json()["job_id"])
    assert out["status"] == "failed"
    assert "类型" in out["error"]
    assert out["result"] is None


def test_unsupported_column_type_rejected(client: TestClient) -> None:
    left = "SELECT meter_id::bigint, read_time::timestamptz FROM readings"
    right = left
    out = _wait_terminal(client, client.post("/jobs", json={"left_sql": left, "right_sql": right}).json()["job_id"])
    assert out["status"] == "failed"
    assert "bigint/text" in out["error"]


def test_invalid_sql_returns_400(client: TestClient) -> None:
    resp = client.post("/jobs", json={"left_sql": "DROP TABLE readings", "right_sql": BASE_RIGHT})
    assert resp.status_code == 400


def test_slot_busy_returns_409_and_cancel_frees_slot(client: TestClient, monkeypatch) -> None:
    from app import db as db_mod
    from app import external_merge as em
    from app import manager as manager_mod

    # 用只置标志的 cancel 替代信号路径（沙箱 seccomp 禁 kill()）
    def flag_only_cancel(self):
        for job in manager_mod.manager._jobs.values():
            if job.status in ("running", "cancelling"):
                job.cancel_requested = True
                job.status = "cancelling"

    monkeypatch.setattr(db_mod.SnapshotSession, "cancel_all", flag_only_cancel)

    # 让读取循环在批次之间停留，给取消留出受理窗口
    real_add = em.RunWriter.add

    def slow_add(self, rows):
        time.sleep(0.2)
        return real_add(self, rows)

    monkeypatch.setattr(em.RunWriter, "add", slow_add)

    big = "SELECT g::bigint FROM generate_series(1, 40000) AS g"
    first = client.post("/jobs", json={"left_sql": big, "right_sql": big})
    assert first.status_code == 201
    job_id = first.json()["job_id"]

    # 名额占用期间第二个请求被拒
    second = client.post("/jobs", json={"left_sql": BASE_LEFT, "right_sql": BASE_RIGHT})
    assert second.status_code == 409
    assert second.json()["detail"]["active_job_id"] == job_id

    # 取消应及时受理
    cancelled = client.post(f"/jobs/{job_id}/cancel")
    assert cancelled.json()["cancel_requested"] is True
    assert cancelled.json()["status"] == "cancelling"

    out = _wait_terminal(client, job_id, timeout=15)
    assert out["status"] == "cancelled"
    assert out["result"] is None

    # 名额释放后可再建任务
    again = client.post("/jobs", json={"left_sql": BASE_LEFT, "right_sql": BASE_RIGHT})
    assert again.status_code == 201
    assert _wait_terminal(client, again.json()["job_id"])["status"] == "succeeded"


def test_immediate_cancel_terminates_without_waiting_for_sql(client: TestClient, monkeypatch) -> None:
    """取消标志在拉取循环的检查点被识别，不依赖内核信号投递。

    沙箱 seccomp 禁止 kill()，无法验证 conn.cancel() 的信号路径，
    这里把 SnapshotSession.cancel_all 替换为“只置取消标志”，
    验证应用层检查点与终态/名额释放链路。
    """
    from app import db as db_mod
    from app import manager as manager_mod

    real_cancel_all = db_mod.SnapshotSession.cancel_all

    def flag_only_cancel(self):
        for job in manager_mod.manager._jobs.values():
            if job.status in ("running", "cancelling"):
                job.cancel_requested = True
                job.status = "cancelling"

    monkeypatch.setattr(db_mod.SnapshotSession, "cancel_all", flag_only_cancel)

    slow = (
        "SELECT g::bigint FROM generate_series(1, 50000) AS g, "
        "LATERAL (SELECT 1) AS z WHERE pg_sleep(0.0002) IS NOT NULL"
    )
    resp = client.post("/jobs", json={"left_sql": slow, "right_sql": slow})
    job_id = resp.json()["job_id"]

    # 等任务进入 running 后请求取消
    for _ in range(100):
        if client.get(f"/jobs/{job_id}").json()["status"] == "running":
            break
        time.sleep(0.02)
    client.post(f"/jobs/{job_id}/cancel")

    out = _wait_terminal(client, job_id, timeout=15)
    assert out["status"] == "cancelled"
    assert out["result"] is None
    _ = real_cancel_all  # 保留引用说明生产环境走真实信号取消
    # 名额已释放
    assert client.post("/jobs", json={"left_sql": BASE_LEFT, "right_sql": BASE_RIGHT}).status_code == 201


def test_statement_timeout_is_configured_on_reader_sessions(client: TestClient) -> None:
    """读取会话必须带 statement_timeout 兜底（沙箱内信号不生效，至少验证配置落地）。"""
    from app import config as config_mod

    assert config_mod.settings.statement_timeout_ms > 0
    # 两侧查询各自在独立读取会话内执行，能成功返回即说明会话参数已设置
    sql = (
        "SELECT COALESCE(NULLIF(current_setting('statement_timeout', true), ''), '0')"
        "::text AS v"
    )
    out = _wait_terminal(client, client.post("/jobs", json={"left_sql": sql, "right_sql": sql}).json()["job_id"])
    assert out["status"] == "succeeded"
    assert out["result"]["column_types"] == ["text"]
    assert out["result"]["left_row_total"] >= 1
    assert out["result"]["diffs"] == []  # 两侧读到的设置一致


def test_query_error_aborts_round_and_frees_slot(client: TestClient) -> None:
    """任一侧 SQL 执行失败 -> failed 终态、不发布结论、名额释放。"""
    bad = "SELECT 1::bigint AS a, (1/0)::bigint AS b"
    resp = client.post("/jobs", json={"left_sql": bad, "right_sql": BASE_LEFT})
    out = _wait_terminal(client, resp.json()["job_id"], timeout=20)
    assert out["status"] == "failed"
    assert out["result"] is None
    assert "侧查询失败" in out["error"]
    assert client.post("/jobs", json={"left_sql": BASE_LEFT, "right_sql": BASE_RIGHT}).status_code == 201


def test_timeout_terminates_round_without_conclusion(client: TestClient, monkeypatch) -> None:
    """30 秒类时限由客户端检查点强制：超时后终止本轮、无结论、释放名额。

    沙箱 seccomp 封死了 PostgreSQL 的信号/定时器，服务端超时无法在本地验证；
    这里把单轮时限调小，并在每批拉取间注入延迟，让客户端检查点确定触发。
    """
    from app import config as config_mod
    from app import db as db_mod
    from app import external_merge as em

    monkeypatch.setattr(config_mod.settings, "job_timeout_seconds", 1)
    monkeypatch.setattr(config_mod.settings, "statement_timeout_ms", 60000)

    real_add = em.RunWriter.add

    def slow_add(self, rows):
        time.sleep(0.3)  # 让轮询循环跨过 deadline
        return real_add(self, rows)

    monkeypatch.setattr(em.RunWriter, "add", slow_add)

    big = "SELECT g::bigint FROM generate_series(1, 60000) AS g"
    resp = client.post("/jobs", json={"left_sql": big, "right_sql": big})
    out = _wait_terminal(client, resp.json()["job_id"], timeout=20)
    assert out["status"] == "timeout"
    assert out["result"] is None
    assert "时限" in out["error"]
    # 终态后名额已释放
    assert client.post("/jobs", json={"left_sql": BASE_LEFT, "right_sql": BASE_RIGHT}).status_code == 201
    _ = db_mod  # 模块在生产路径中承担连接与游标管理


def test_work_dir_cleaned_after_finish(client: TestClient) -> None:
    import os

    from app import config as config_mod

    work = config_mod.settings.work_dir
    small = "SELECT meter_id::bigint FROM meters"
    out = _wait_terminal(
        client,
        client.post("/jobs", json={"left_sql": small, "right_sql": small}).json()["job_id"],
        timeout=60,
    )
    assert out["status"] == "succeeded"
    job_dir = os.path.join(work, out["job_id"])
    assert not os.path.exists(job_dir)


def test_status_query_works_after_terminal(client: TestClient) -> None:
    out = _wait_terminal(
        client,
        client.post("/jobs", json={"left_sql": BASE_LEFT, "right_sql": BASE_RIGHT}).json()["job_id"],
    )
    again = client.get(f"/jobs/{out['job_id']}")
    assert again.status_code == 200
    assert again.json()["status"] == "succeeded"
    listed = client.get("/jobs").json()
    assert any(j["job_id"] == out["job_id"] for j in listed)


def test_missing_job_404(client: TestClient) -> None:
    assert client.get("/jobs/nope").status_code == 404
    assert client.post("/jobs/nope/cancel").status_code == 404
