"""任务收尾语义的确定性回归测试（不依赖真实数据库）。

用可控的假会话 / 假读取函数制造交错，覆盖：
- 任一侧失败 -> 立即中止另一侧，终态保留原始失败原因（不被超时/取消覆盖）；
- 取消先受理 -> 不发布结论；SQL 尚未派发时不再派发；
- 成功先发布 -> 保持 succeeded 并明确告知不可取消；
- 状态与 result 只能在同一次完整发布后一起可见；
- 全部读取线程退出、暂存清空之后才报终态；慢收尾不阻塞查询与取消受理。
"""
from __future__ import annotations

import os
import shutil
import threading
import time

import pytest

from app import manager as manager_mod
from app.db import ColumnMeta
from app.external_merge import JobCancelled, RunWriter
from app.manager import JobManager, SlotBusyError
from app.models import TERMINAL_STATUSES

COLUMNS = [ColumnMeta(name="v", pg_type="bigint")]


class FakeSession:
    """假的快照会话：记录取消/终止调用，提供跨线程 cancel 事件。"""

    instances: list["FakeSession"] = []

    def __init__(self) -> None:
        self.readers = [object(), object()]
        self.cancel_event = threading.Event()
        self.cancel_calls = 0
        self.terminate_calls = 0
        self.closed = False
        FakeSession.instances.append(self)

    def open(self) -> None:
        pass

    def cancel_all(self) -> None:
        self.cancel_calls += 1
        self.cancel_event.set()

    def terminate_backends(self) -> None:
        self.terminate_calls += 1
        self.cancel_event.set()

    def close(self) -> None:
        self.closed = True


def _write_rows(work_dir: str, rows: list[tuple]) -> tuple[list[str], int]:
    w = RunWriter(work_dir=work_dir, chunk_rows=100)
    if rows:
        w.add(rows)
    return w.finish(), len(rows)


def _install(monkeypatch, tmp_path):
    """安装假会话与可控 describe；返回调用记录。"""
    calls = {"describe": 0}
    FakeSession.instances = []
    monkeypatch.setattr(manager_mod.dbmod, "SnapshotSession", FakeSession)
    monkeypatch.setattr(manager_mod.settings, "work_dir", str(tmp_path))
    return calls


def _wait_status(job, statuses, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if job.status in statuses:
            return
        time.sleep(0.005)
    raise AssertionError(f"任务未进入 {statuses}，当前 {job.status}")


def _wait(predicate, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("条件未在时限内满足")


def test_first_side_failure_stops_other_side_and_keeps_reason(monkeypatch, tmp_path) -> None:
    """左失败后右必须被立即中止；终态 failed、保留左的原始异常，而非 timeout/cancelled。"""
    _install(monkeypatch, tmp_path)

    def fake_describe(*, conn, sql, cursor_name, work_dir, chunk_rows,
                      is_cancelled, deadline_check):
        if "recon_l_" in cursor_name:
            raise RuntimeError("left boom")
        # 右侧本可“跑很久”：只在中止信号到达时退出
        session = FakeSession.instances[-1]
        session.cancel_event.wait(timeout=10)
        raise JobCancelled()

    monkeypatch.setattr(manager_mod.dbmod, "describe_and_stream", fake_describe)
    m = JobManager()
    t0 = time.time()
    job = m.create_job("SELECT 1::bigint", "SELECT 1::bigint", "race")
    _wait_status(job, TERMINAL_STATUSES, timeout=3)
    elapsed = time.time() - t0

    assert job.status == "failed"
    assert job.result is None
    assert job.error is not None and job.error.startswith("left 侧查询失败")
    assert "left boom" in job.error
    assert "取消" not in job.error and "时限" not in job.error
    assert elapsed < 3, "另一侧未被及时中止"
    assert m._active is None
    assert job.work_dir is not None and not os.path.exists(job.work_dir)


def test_failure_reason_survives_later_timeout(monkeypatch, tmp_path) -> None:
    _install(monkeypatch, tmp_path)

    def fake_describe(*, conn, sql, cursor_name, work_dir, chunk_rows,
                      is_cancelled, deadline_check):
        if "recon_l_" in cursor_name:
            raise RuntimeError("disk read failed")
        session = FakeSession.instances[-1]
        # 忽略中止，稍后自己超时退出；期间测试线程会推后 deadline 触发看门狗
        session.cancel_event.wait(timeout=0.5)
        raise JobCancelled()

    monkeypatch.setattr(manager_mod.dbmod, "describe_and_stream", fake_describe)
    m = JobManager()
    job = m.create_job("SELECT 1::bigint", "SELECT 1::bigint", "timeout-late")
    # 等失败已被记录，再模拟时间到期（看门狗同样走 _mark_timeout）
    _wait(lambda: job.failure_seq is not None, timeout=3)
    m._mark_timeout(job)
    _wait_status(job, TERMINAL_STATUSES, timeout=3)

    assert job.status == "failed"
    assert job.error is not None and "disk read failed" in job.error
    assert job.result is None


def test_cancel_before_sql_dispatch_never_runs_query(monkeypatch, tmp_path) -> None:
    calls = _install(monkeypatch, tmp_path)
    opened = threading.Event()
    release = threading.Event()

    class GatedSession(FakeSession):
        def open(self) -> None:
            opened.set()
            release.wait(timeout=5)

    monkeypatch.setattr(manager_mod.dbmod, "SnapshotSession", GatedSession)

    def fake_describe(**kwargs):
        calls["describe"] += 1
        work_dir = kwargs["work_dir"]
        runs, total = _write_rows(work_dir, [(1,)])
        return COLUMNS, runs, total

    monkeypatch.setattr(manager_mod.dbmod, "describe_and_stream", fake_describe)
    m = JobManager()
    job = m.create_job("SELECT 1::bigint", "SELECT 1::bigint", "cancel-pending")
    assert opened.wait(2), "任务未开始打开会话"

    cancelled, accepted = m.cancel_job_request(job.job_id)
    assert accepted is True
    assert cancelled.status == "cancelling"
    assert cancelled.result is None

    release.set()
    _wait_status(job, TERMINAL_STATUSES, timeout=3)
    assert job.status == "cancelled"
    assert job.result is None
    assert calls["describe"] == 0, "取消先受理后不应再派发 SQL"
    assert m._active is None


def test_cancel_accepted_while_running_publishes_no_result(monkeypatch, tmp_path) -> None:
    _install(monkeypatch, tmp_path)
    gate_release = threading.Event()
    right_started = threading.Event()

    def fake_describe(*, conn, sql, cursor_name, work_dir, chunk_rows,
                      is_cancelled, deadline_check):
        if "recon_r_" in cursor_name:
            right_started.set()
            gate_release.wait(timeout=5)  # 若不被中止，会成功产出结果
        runs, total = _write_rows(work_dir, [(7,)])
        return COLUMNS, runs, total

    monkeypatch.setattr(manager_mod.dbmod, "describe_and_stream", fake_describe)
    m = JobManager()
    job = m.create_job("SELECT 7::bigint", "SELECT 7::bigint", "cancel-run")
    assert right_started.wait(3)

    t0 = time.time()
    cancelled, accepted = m.cancel_job_request(job.job_id)
    elapsed = time.time() - t0
    assert accepted is True
    assert elapsed < 0.5, "取消受理被阻塞"
    assert cancelled.status == "cancelling"
    assert cancelled.result is None

    gate_release.set()
    _wait_status(job, TERMINAL_STATUSES, timeout=3)
    assert job.status == "cancelled"
    assert job.result is None
    assert m._active is None


def test_success_published_first_cannot_be_cancelled(monkeypatch, tmp_path) -> None:
    _install(monkeypatch, tmp_path)

    def fake_describe(*, conn, sql, cursor_name, work_dir, chunk_rows,
                      is_cancelled, deadline_check):
        runs, total = _write_rows(work_dir, [(1,), (2,)])
        return COLUMNS, runs, total

    monkeypatch.setattr(manager_mod.dbmod, "describe_and_stream", fake_describe)
    m = JobManager()
    job = m.create_job("SELECT 1::bigint", "SELECT 1::bigint", "already-done")
    _wait_status(job, TERMINAL_STATUSES, timeout=3)
    assert job.status == "succeeded" and job.result is not None

    returned, accepted = m.cancel_job_request(job.job_id)
    assert accepted is False
    assert returned.status == "succeeded"
    assert returned.result is not None and returned.result.identical is True
    assert returned.cancel_requested is False


def test_atomic_publication_and_slow_cleanup_does_not_block(monkeypatch, tmp_path) -> None:
    """暂存清理被卡住时：状态仍非终态且无结论；查询/取消即时返回；
    清理完成后才原子发布终态。并发轮询不得观察到撕裂状态。"""
    _install(monkeypatch, tmp_path)

    def fake_describe(*, conn, sql, cursor_name, work_dir, chunk_rows,
                      is_cancelled, deadline_check):
        runs, total = _write_rows(work_dir, [(42,)])
        return COLUMNS, runs, total

    monkeypatch.setattr(manager_mod.dbmod, "describe_and_stream", fake_describe)

    real_rmtree = shutil.rmtree
    cleanup_entered = threading.Event()
    cleanup_release = threading.Event()

    def slow_rmtree(path, *args, **kwargs):
        cleanup_entered.set()
        cleanup_release.wait(timeout=10)
        real_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(manager_mod.shutil, "rmtree", slow_rmtree)

    m = JobManager()
    job = m.create_job("SELECT 42::bigint", "SELECT 42::bigint", "slow-cleanup")

    violations: list[str] = []
    stop_poll = threading.Event()

    def poll() -> None:
        while not stop_poll.is_set():
            snap = job.snapshot()
            if snap.status == "succeeded" and snap.result is None:
                violations.append("succeeded 无结论")
            if snap.status in ("pending", "running", "cancelling") and snap.result is not None:
                violations.append(f"{snap.status} 已有结论")
            if snap.status in TERMINAL_STATUSES and not cleanup_entered.is_set():
                violations.append("暂存清空前已报终态")
            time.sleep(0.001)

    poller = threading.Thread(target=poll, daemon=True)
    poller.start()
    try:
        assert cleanup_entered.wait(3), "收尾未进入暂存清理阶段"

        # 慢收尾期间：状态非终态、无结论；查询与取消即时返回
        snap = job.snapshot()
        assert snap.status in ("running", "cancelling")
        assert snap.result is None
        t0 = time.time()
        _ = m.get_job(job.job_id).snapshot()
        returned, accepted = m.cancel_job_request(job.job_id)
        assert time.time() - t0 < 0.5, "慢收尾阻塞了查询或取消受理"
        assert accepted is True
        assert returned.status == "cancelling" and returned.result is None

        # 名额尚未释放（SQL/暂存收尾未完成），新任务应被拒
        with pytest.raises(SlotBusyError):
            m.create_job("SELECT 1::bigint", "SELECT 1::bigint", "too-early")

        cleanup_release.set()
        _wait_status(job, TERMINAL_STATUSES, timeout=5)
    finally:
        cleanup_release.set()
        stop_poll.set()
        poller.join(timeout=2)

    assert violations == []
    # 取消先于发布被受理 -> 保持不发布结论
    assert job.status == "cancelled"
    assert job.result is None
    assert m._active is None
    assert job.work_dir is not None and not os.path.exists(job.work_dir)


def test_success_terminal_only_after_cleanup_and_readers_exit(monkeypatch, tmp_path) -> None:
    """成功分支同样：暂存清空、读取线程退出后才出现 succeeded + result。"""
    _install(monkeypatch, tmp_path)
    real_rmtree = shutil.rmtree
    cleanup_entered = threading.Event()
    cleanup_release = threading.Event()

    def fake_describe(*, conn, sql, cursor_name, work_dir, chunk_rows,
                      is_cancelled, deadline_check):
        runs, total = _write_rows(work_dir, [(1,)])
        return COLUMNS, runs, total

    def slow_rmtree(path, *args, **kwargs):
        cleanup_entered.set()
        cleanup_release.wait(timeout=10)
        real_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(manager_mod.dbmod, "describe_and_stream", fake_describe)
    monkeypatch.setattr(manager_mod.shutil, "rmtree", slow_rmtree)

    m = JobManager()
    job = m.create_job("SELECT 1::bigint", "SELECT 1::bigint", "success-slow")
    assert cleanup_entered.wait(3)
    snap = job.snapshot()
    assert snap.status not in TERMINAL_STATUSES and snap.result is None
    assert all(not t.is_alive() for t in job.reader_threads), "读取线程未退出就开始收尾"
    assert os.path.exists(job.work_dir or ""), "暂存尚未清空却已进入终态发布"

    cleanup_release.set()
    _wait_status(job, TERMINAL_STATUSES, timeout=5)
    assert job.status == "succeeded"
    assert job.result is not None
    assert m._active is None
    assert not os.path.exists(job.work_dir or "")
