"""核对任务的生命周期编排。

并发模型：
- 进程内全局只允许一个活动任务（单名额），新建任务若名额占用返回 409；
- 工作线程内开两条读取线程并行拉取两侧结果，共享同一快照；
- 看门狗线程负责 30s 超时；取消 / 超时 / 任一侧失败都会
  cancel（必要时 terminate）本轮全部 SQL，待线程退出、暂存清空后才释放名额；
- 终态后不发布任何“比较结论”的途径只有状态接口里的 result 字段：
  非 succeeded 时 result 恒为 null。
"""
from __future__ import annotations

import os
import shutil
import threading
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from . import db as dbmod
from .config import settings
from .external_merge import JobCancelled, reconcile_sorted
from .models import TERMINAL_STATUSES, JobOut, ReconcileResult, DiffRow
from .sqlvalidation import validate_select


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class JobTimeout(Exception):
    pass


@dataclass
class Job:
    job_id: str
    name: str | None
    left_sql: str
    right_sql: str
    status: str = "pending"
    created_at: datetime = field(default_factory=_utcnow)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    deadline: datetime | None = None
    cancel_requested: bool = False
    error: str | None = None
    result: ReconcileResult | None = None
    # 运行期句柄（不进 API 输出）
    work_dir: str | None = None
    session: dbmod.SnapshotSession | None = None
    worker_thread: threading.Thread | None = None
    reader_threads: list[threading.Thread] = field(default_factory=list)
    abort_done: bool = False
    timed_out: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def snapshot(self) -> JobOut:
        return JobOut(
            job_id=self.job_id,
            name=self.name,
            status=self.status,
            created_at=self.created_at,
            started_at=self.started_at,
            finished_at=self.finished_at,
            deadline=self.deadline,
            cancel_requested=self.cancel_requested,
            error=self.error,
            result=self.result,
        )


class JobManager:
    def __init__(self) -> None:
        self._slot_lock = threading.Lock()       # 保护名额与注册表
        self._active: Job | None = None
        self._last_terminal: Job | None = None
        self._jobs: dict[str, Job] = {}
        os.makedirs(settings.work_dir, exist_ok=True)

    # ---------- 查询 ----------
    def get_job(self, job_id: str) -> Job | None:
        with self._slot_lock:
            return self._jobs.get(job_id)

    def list_jobs(self) -> list[JobOut]:
        with self._slot_lock:
            return [j.snapshot() for j in self._jobs.values()]

    # ---------- 创建 ----------
    def create_job(self, left_sql: str, right_sql: str, name: str | None) -> Job:
        # 先做静态校验，非法 SQL 直接 400，不占名额
        left_norm = validate_select(left_sql)
        right_norm = validate_select(right_sql)

        job = Job(
            job_id=uuid.uuid4().hex,
            name=name,
            left_sql=left_norm,
            right_sql=right_norm,
        )
        with self._slot_lock:
            if self._active is not None:
                raise SlotBusyError(self._active.job_id)
            self._active = job
            self._jobs[job.job_id] = job

        thread = threading.Thread(
            target=self._run_guarded, args=(job,), name=f"recon-{job.job_id[:8]}", daemon=True
        )
        job.worker_thread = thread
        thread.start()
        return job

    # ---------- 取消 ----------
    def request_cancel(self, job_id: str) -> Job:
        with self._slot_lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(job_id)
            if job.status in TERMINAL_STATUSES:
                return job
            job.cancel_requested = True
            job.status = "cancelling"
            session = job.session
        # 锁外触发网络取消
        if session is not None:
            session.cancel_all()
        return job

    # ---------- 主流程 ----------
    def _run_guarded(self, job: Job) -> None:
        try:
            self._run(job)
        except Exception as exc:  # 兜底：任何未预期异常都要走到终态并释放名额
            traceback.print_exc()
            self._finalize(
                job,
                status="failed",
                error=f"内部错误：{exc.__class__.__name__}: {exc}",
                result=None,
            )

    def _run(self, job: Job) -> None:
        job.started_at = _utcnow()
        job.deadline = job.started_at + timedelta(seconds=settings.job_timeout_seconds)
        job.status = "running"
        job.work_dir = tempdir = os.path.join(settings.work_dir, job.job_id)
        os.makedirs(tempdir, exist_ok=True)

        def is_cancelled() -> bool:
            return job.cancel_requested

        def check_deadline() -> None:
            if job.timed_out or (
                job.deadline is not None and _utcnow() >= job.deadline
            ):
                job.timed_out = True
                raise JobTimeout()

        watchdog = threading.Thread(
            target=self._watchdog, args=(job,), name=f"watchdog-{job.job_id[:8]}", daemon=True
        )
        watchdog.start()

        session = dbmod.SnapshotSession()
        job.session = session
        try:
            session.open()
            if is_cancelled():
                raise JobCancelled()
            check_deadline()

            left_out: dict[str, Any] = {}
            right_out: dict[str, Any] = {}
            t_left = threading.Thread(
                target=self._read_side,
                args=(session.readers[0], job.left_sql, f"recon_l_{job.job_id[:8]}",
                      tempdir, "left", left_out, is_cancelled, check_deadline),
                name="reader-left",
                daemon=True,
            )
            t_right = threading.Thread(
                target=self._read_side,
                args=(session.readers[1], job.right_sql, f"recon_r_{job.job_id[:8]}",
                      tempdir, "right", right_out, is_cancelled, check_deadline),
                name="reader-right",
                daemon=True,
            )
            job.reader_threads = [t_left, t_right]
            t_left.start()
            t_right.start()
            # 有界等待：轮询读取线程与取消/超时信号，避免卡在阻塞中的 fetchmany
            while t_left.is_alive() or t_right.is_alive():
                if is_cancelled():
                    raise JobCancelled()
                check_deadline()  # 看门狗置位或时间到期 -> timeout
                t_left.join(timeout=0.1)
                t_right.join(timeout=0.1)

            # 任一侧异常 -> 本轮失败（超时/取消优先归类）
            for side, out in (("left", left_out), ("right", right_out)):
                if "error" in out:
                    err = out["error"]
                    if isinstance(err, JobTimeout) or job.timed_out:
                        raise JobTimeout()
                    if isinstance(err, JobCancelled) or job.cancel_requested:
                        raise JobCancelled()
                    raise dbmod.DbError(f"{side} 侧查询失败：{err}")

            check_deadline()
            left_cols: list[dbmod.ColumnMeta] = left_out["columns"]
            right_cols: list[dbmod.ColumnMeta] = right_out["columns"]
            self._check_schema(left_cols, right_cols)

            stats = reconcile_sorted(
                left_out["runs"],
                right_out["runs"],
                left_total=left_out["total"],
                right_total=right_out["total"],
                max_diff_types=settings.max_diff_types,
                is_cancelled=is_cancelled,
                work_dir=tempdir,
            )
            check_deadline()
            if is_cancelled():
                raise JobCancelled()

            result = ReconcileResult(
                identical=stats.diff_group_total == 0,
                columns=[c.name for c in left_cols],
                column_types=[c.pg_type for c in left_cols],
                left_row_total=stats.left_row_total,
                right_row_total=stats.right_row_total,
                matched_group_count=stats.matched_group_count,
                diff_group_total=stats.diff_group_total,
                diffs_shown=len(stats.diffs),
                diffs=[DiffRow(**d) for d in stats.diffs],
            )
        except JobCancelled:
            self._finalize(job, status="cancelled",
                           error="本轮已取消，未发布比较结论", result=None)
            return
        except JobTimeout:
            self._finalize(
                job,
                status="timeout",
                error=f"超过单轮 {settings.job_timeout_seconds} 秒时限，"
                      f"已终止本轮全部 SQL，未发布比较结论",
                result=None,
            )
            return
        except dbmod.DbError as exc:
            self._finalize(job, status="failed", error=str(exc), result=None)
            return
        finally:
            # 让看门狗退出并断开数据库连接（无论走哪条终态分支）
            job.deadline = None
            watchdog.join(timeout=2)
            # finalize 内也会 close；这里兜底处理成功分支之外的提前退出
            if job.session is not None:
                session.close()
                job.session = None

        # 成功路径：有结论，按同一收尾顺序（清暂存、放名额、再报终态）
        self._finalize(job, status="succeeded", error=None, result=result)

    def _read_side(self, conn, sql, cursor_name, tempdir, side, out,
                   is_cancelled, check_deadline) -> None:
        try:
            columns, runs, total = dbmod.describe_and_stream(
                conn=conn,
                sql=sql,
                cursor_name=cursor_name,
                work_dir=tempdir,
                chunk_rows=settings.chunk_rows,
                is_cancelled=is_cancelled,
                deadline_check=check_deadline,
            )
            out.update(columns=columns, runs=runs, total=total)
        except Exception as exc:  # noqa: BLE001 - 跨线程回传，由主线程统一分类
            out["error"] = exc

    @staticmethod
    def _check_schema(left: list[dbmod.ColumnMeta], right: list[dbmod.ColumnMeta]) -> None:
        if len(left) != len(right):
            raise dbmod.DbError(
                f"两侧列数不同：left={len(left)} 列，right={len(right)} 列"
            )
        for idx, (lc, rc) in enumerate(zip(left, right)):
            if lc.pg_type != rc.pg_type:
                raise dbmod.DbError(
                    f"第 {idx + 1} 列类型不匹配：left={lc.name}({lc.pg_type})，"
                    f"right={rc.name}({rc.pg_type})"
                )

    def _watchdog(self, job: Job) -> None:
        """到点未结束则标记超时并请求中止 SQL（不改写用户取消标志）。"""
        while True:
            deadline = job.deadline
            if deadline is None:
                return
            remaining = (deadline - _utcnow()).total_seconds()
            if remaining <= 0:
                if job.status not in TERMINAL_STATUSES:
                    job.timed_out = True
                    if job.session is not None:
                        job.session.cancel_all()
                return
            time.sleep(min(0.2, remaining))

    def _finalize(self, job: Job, *, status: str, error: str | None,
                  result: ReconcileResult | None) -> None:
        """统一收尾，严格按需求顺序执行：

        1) 中止本轮全部 SQL（有界等待，受限环境下不阻塞收尾）；
        2) 关闭数据库连接、结束看门狗；
        3) 清空暂存目录；
        4) 释放唯一运行名额；
        5) 最后才落终态并对外可见（失败/取消/超时一律不发布比较结论）。
        """
        # 已终态（理论上不会发生）则幂等返回
        if job.status in TERMINAL_STATUSES:
            return

        session = job.session
        if session is not None and status != "succeeded":
            self._abort_sql(job, session)

        job.deadline = None
        if session is not None:
            session.close()
            job.session = None

        # 清暂存
        if job.work_dir:
            shutil.rmtree(job.work_dir, ignore_errors=True)

        # 释放名额（持锁期间只做内存操作，不做 IO）
        with self._slot_lock:
            if self._active is job:
                self._active = None
            self._last_terminal = job
            if len(self._jobs) > 20:
                for jid in list(self._jobs):
                    if self._jobs[jid].status in TERMINAL_STATUSES and jid != job.job_id:
                        self._jobs.pop(jid, None)
                        break

        # 最后报终态
        job.error = error
        job.result = result
        job.finished_at = _utcnow()
        job.status = status

    def _abort_sql(self, job: Job, session: dbmod.SnapshotSession) -> None:
        """尽力中止本轮 SQL，但绝不无限期阻塞名额释放。

        顺序：cancel（请求查询取消信号）-> 有界等待读取线程 ->
        terminate 兜底 -> 再给一次有界等待。仍未退出的线程只可能阻塞在
        网络读上：连接将在 _run 的 finally 中关闭，暂存清理与名额释放
        不依赖线程完全退出（受限环境下信号可能无法投递）。
        本方法在 worker 线程内调用，绝不能 join worker 自己。
        """
        if job.abort_done:
            return
        job.abort_done = True
        session.cancel_all()
        for t in job.reader_threads:
            if t.is_alive():
                t.join(timeout=2)
        if any(t.is_alive() for t in job.reader_threads):
            session.terminate_backends()
            for t in job.reader_threads:
                t.join(timeout=2)

class SlotBusyError(RuntimeError):
    def __init__(self, active_job_id: str):
        super().__init__(f"已有核对任务在运行：job_id={active_job_id}")
        self.active_job_id = active_job_id


# 模块级单例
manager = JobManager()
