"""核对任务的生命周期编排。

并发模型：
- 进程内全局只允许一个活动任务（单名额），新建任务若名额占用返回 409；
- 工作线程内开两条读取线程并行拉取两侧结果，共享同一快照；
- 看门狗线程负责 30s 超时；取消 / 超时 / 任一侧失败都会
  cancel（必要时 terminate）本轮全部 SQL；
- 收尾必须等全部读取线程退出、连接关闭、暂存清空之后，才原子地
  发布终态并释放名额。状态与 result 在同一次持锁发布中写入，
  查询接口只能在同一个快照里看到二者，绝不会出现 running 已有 result
  或 succeeded 没有 result 的撕裂状态；
- 终止原因按“第一个到达的事件”裁决：任一侧的真实失败优先于随后到达的
  超时或取消，取消优先于尚未发布成功的收尾；成功一旦发布，后续取消
  一律不受理并明确告知。
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
from .models import (
    TERMINAL_STATUSES,
    DiffRow,
    JobOut,
    ReconcileResult,
)
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
    watchdog_thread: threading.Thread | None = None
    reader_threads: list[threading.Thread] = field(default_factory=list)

    # 终止事件：failure / cancel / timeout 三者按序号先后裁决。
    # 所有字段的读写都在 _lock 下进行；网络操作一律在锁外完成。
    failure: tuple[str | None, BaseException] | None = None
    failure_seq: int | None = None
    cancel_seq: int | None = None
    timeout_seq: int | None = None
    timed_out: bool = False
    abort_event: threading.Event = field(default_factory=threading.Event)
    _stop_watchdog: threading.Event = field(default_factory=threading.Event)
    _event_seq: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def snapshot(self) -> JobOut:
        """在锁内一次性复制状态与结论，保证二者来自同一次发布。"""
        with self._lock:
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
        # 只在锁内取引用，snapshot 的锁在调用方完成，避免锁顺序反转
        with self._slot_lock:
            return self._jobs.get(job_id)

    def list_jobs(self) -> list[JobOut]:
        with self._slot_lock:
            jobs = list(self._jobs.values())
        return [j.snapshot() for j in jobs]

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
        """兼容旧接口：返回任务；是否受理见 cancel_job_request。"""
        job, _accepted = self.cancel_job_request(job_id)
        return job

    def cancel_job_request(self, job_id: str) -> tuple[Job, bool]:
        """受理取消（非阻塞）。

        返回 (job, accepted)。终态任务不再改变状态，accepted=False 表示
        “结论已经发布，无法取消”；成功已发布时保持 succeeded 与 result。
        这里只在内存锁内改标志，网络 cancel 在锁外触发，因此收尾再慢
        也不会阻塞取消受理。
        """
        with self._slot_lock:
            job = self._jobs.get(job_id)
        if job is None:
            raise KeyError(job_id)

        with job._lock:
            if job.status in TERMINAL_STATUSES:
                return job, False
            if job.cancel_seq is None:
                job._event_seq += 1
                job.cancel_seq = job._event_seq
            job.cancel_requested = True
            if job.status in ("pending", "running"):
                job.status = "cancelling"
            job.abort_event.set()
            session = job.session

        # 锁外触发网络取消（可从控制线程调用）
        if session is not None:
            session.cancel_all()
        return job, True

    # ---------- 事件裁决 ----------
    def _mark_timeout(self, job: Job) -> None:
        """记录超时；若更早的真实失败已经记录，则保留失败原因。"""
        with job._lock:
            if job.status in TERMINAL_STATUSES:
                return
            if job.failure_seq is not None or job.timeout_seq is not None:
                return
            job._event_seq += 1
            job.timeout_seq = job._event_seq
            job.timed_out = True
            job.abort_event.set()

    def _record_failure(self, job: Job, side: str | None, exc: BaseException) -> None:
        """记录本轮第一个真实失败。

        取消 / 超时 / 另一侧失败引起的连带异常（JobCancelled、查询被取消、
        连接被终止等）不覆盖已经确定的原因；只有更早发生的真实失败才保留。
        """
        if isinstance(exc, (JobCancelled, JobTimeout)):
            return
        with job._lock:
            if job.status in TERMINAL_STATUSES:
                return
            if job.cancel_seq is not None or job.timeout_seq is not None:
                return
            if job.failure_seq is not None:
                return
            job._event_seq += 1
            job.failure_seq = job._event_seq
            job.failure = (side, exc)
            # 任一侧失败 -> 立即通知另一侧停止
            job.abort_event.set()

    def _check_deadline(self, job: Job) -> None:
        deadline = job.deadline
        if deadline is not None and _utcnow() >= deadline:
            self._mark_timeout(job)
        with job._lock:
            timed_out = job.timed_out
        if timed_out:
            raise JobTimeout()

    # ---------- 主流程 ----------
    def _run_guarded(self, job: Job) -> None:
        candidate: ReconcileResult | None = None
        try:
            candidate = self._execute(job)
        except (JobCancelled, JobTimeout):
            # 取消/超时是正常控制流，由 _publish 依据事件序号裁决
            pass
        except dbmod.DbError as exc:
            # 预期内的查询/类型失败：保留原因，不打整段堆栈
            self._record_failure(job, None, exc)
        except Exception as exc:  # 兜底：任何未预期异常都要走到终态并释放名额
            traceback.print_exc()
            self._record_failure(job, None, exc)
        finally:
            # 收尾顺序固定：停止 SQL -> 等待全部读取线程退出 -> 关闭连接
            # -> 清空暂存 -> 才允许发布终态/释放名额。
            try:
                self._shutdown(job)
            except Exception:  # 收尾异常不能阻止终态，但必须保留日志
                traceback.print_exc()
            self._publish(job, candidate)

    def _execute(self, job: Job) -> ReconcileResult | None:
        # pending -> running 与取消受理在同一把锁下裁决：
        # 取消先到则不再启动任何 SQL。
        with job._lock:
            if job.status in TERMINAL_STATUSES:
                return None
            if job.cancel_seq is not None or job.abort_event.is_set():
                return None
            job.started_at = _utcnow()
            job.deadline = job.started_at + timedelta(
                seconds=settings.job_timeout_seconds
            )
            job.status = "running"

        job.work_dir = tempdir = os.path.join(settings.work_dir, job.job_id)
        os.makedirs(tempdir, exist_ok=True)

        watchdog = threading.Thread(
            target=self._watchdog, args=(job,), name=f"watchdog-{job.job_id[:8]}", daemon=True
        )
        job.watchdog_thread = watchdog
        watchdog.start()

        session = dbmod.SnapshotSession()
        with job._lock:
            job.session = session

        # 取消/超时可能在挂上 session 之前到达
        if job.abort_event.is_set():
            return None

        session.open()
        if job.abort_event.is_set():
            return None

        left_out: dict[str, Any] = {}
        right_out: dict[str, Any] = {}

        def is_aborted() -> bool:
            # abort_event 覆盖失败/超时/取消三条通道；cancel_requested
            # 兼容直接置标志的外部取消，二者都立即生效。
            return job.abort_event.is_set() or job.cancel_requested

        def check_deadline() -> None:
            self._check_deadline(job)

        t_left = threading.Thread(
            target=self._read_side,
            args=(job, session.readers[0], job.left_sql, f"recon_l_{job.job_id[:8]}",
                  tempdir, "left", left_out, is_aborted, check_deadline),
            name="reader-left",
            daemon=True,
        )
        t_right = threading.Thread(
            target=self._read_side,
            args=(job, session.readers[1], job.right_sql, f"recon_r_{job.job_id[:8]}",
                  tempdir, "right", right_out, is_aborted, check_deadline),
            name="reader-right",
            daemon=True,
        )
        job.reader_threads = [t_left, t_right]
        t_left.start()
        t_right.start()

        # 有界轮询：任一侧失败/取消/超时都会置 abort_event，
        # 从而立即向另一侧发送 SQL 取消，而不是等两侧都跑完。
        while t_left.is_alive() or t_right.is_alive():
            check_deadline()
            if job.abort_event.wait(0.1):
                session.cancel_all()
                break

        # 终态发布前必须确认两条读取线程都已真正退出
        self._stop_readers(job, session)
        if job.abort_event.is_set():
            return None
        check_deadline()

        if "error" in left_out or "error" in right_out:
            # 防御：正常情况下读取异常会先记录 failure 并置 abort_event
            for side, out in (("left", left_out), ("right", right_out)):
                if "error" in out:
                    self._record_failure(job, side, out["error"])
            return None

        left_cols: list[dbmod.ColumnMeta] = left_out["columns"]
        right_cols: list[dbmod.ColumnMeta] = right_out["columns"]
        self._check_schema(left_cols, right_cols)

        stats = reconcile_sorted(
            left_out["runs"],
            right_out["runs"],
            left_total=left_out["total"],
            right_total=right_out["total"],
            max_diff_types=settings.max_diff_types,
            is_cancelled=is_aborted,
            work_dir=tempdir,
        )
        check_deadline()
        if job.abort_event.is_set():
            return None

        candidate = ReconcileResult(
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
        # 数据侧工作已全部完成：停止计时。后续收尾（等待线程退出、清暂存）
        # 只影响资源释放，不应把一份已算出的结论改判为超时。
        self._stop_timing(job)
        return candidate

    def _stop_timing(self, job: Job) -> None:
        with job._lock:
            job.deadline = None
        job._stop_watchdog.set()

    def _read_side(self, job, conn, sql, cursor_name, tempdir, side, out,
                   is_aborted, check_deadline) -> None:
        try:
            columns, runs, total = dbmod.describe_and_stream(
                conn=conn,
                sql=sql,
                cursor_name=cursor_name,
                work_dir=tempdir,
                chunk_rows=settings.chunk_rows,
                is_cancelled=is_aborted,
                deadline_check=check_deadline,
            )
            out.update(columns=columns, runs=runs, total=total)
        except Exception as exc:  # noqa: BLE001 - 跨线程回传，由主线程统一分类
            out["error"] = exc
            # 任一侧失败立即置 abort_event，主线程随即停止另一侧 SQL；
            # 若失败只是取消/超时的连带结果，_record_failure 会忽略它。
            self._record_failure(job, side, exc)

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
        """到点未结束则记录超时并请求中止 SQL（不覆盖更早的真实失败）。"""
        while not job._stop_watchdog.is_set():
            with job._lock:
                deadline = job.deadline
                finished = job.status in TERMINAL_STATUSES or deadline is None
                session = job.session
            if finished:
                return
            remaining = (deadline - _utcnow()).total_seconds()
            if remaining <= 0:
                self._mark_timeout(job)
                with job._lock:
                    session = job.session
                if session is not None:
                    session.cancel_all()
                return
            # 用事件等待代替 sleep，收尾时可立即唤醒退出
            job._stop_watchdog.wait(min(0.2, remaining))

    def _stop_readers(self, job: Job, session: dbmod.SnapshotSession | None) -> None:
        """中止并等待两条读取线程真正退出。

        升级路径：cancel() -> pg_terminate_backend；服务端 statement_timeout
        是最后兜底，因此等待是有界的。线程全部退出前绝不发布终态；
        该方法只被 worker 线程调用且不持有名额锁，因此慢收尾不会阻塞
        状态查询与取消受理。
        """
        threads = [t for t in job.reader_threads if t is not None]
        if not threads:
            return
        if not any(t.is_alive() for t in threads):
            return
        if session is not None:
            session.cancel_all()
        terminated = False
        started = time.monotonic()
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(timeout=0.05)
            # 取消信号未生效时升级为终止后端；连接随后由 close() 统一关闭，
            # 不在其它线程仍使用连接时跨线程强制关闭（psycopg 非线程安全）。
            if not terminated and time.monotonic() - started >= 0.5:
                if session is not None:
                    session.terminate_backends()
                terminated = True

    def _shutdown(self, job: Job) -> None:
        """等 SQL 与读取线程全部退出、关闭连接、清空暂存。

        这些步骤都不持有名额锁/查询锁，因此即使暂存清理很慢，状态查询与
        取消受理仍然即时返回。
        """
        with job._lock:
            job.deadline = None
            session = job.session
            watchdog = job.watchdog_thread
            aborted = job.abort_event.is_set()

        # 先唤醒看门狗，避免它在下一次迭代再触发 cancel_all
        job._stop_watchdog.set()
        if watchdog is not None and watchdog.is_alive():
            watchdog.join(timeout=2)

        if session is not None:
            if aborted:
                session.cancel_all()
            self._stop_readers(job, session)
            session.close()
            with job._lock:
                job.session = None

        if job.work_dir:
            try:
                shutil.rmtree(job.work_dir, ignore_errors=True)
            except Exception:  # 清理失败不能阻止终态，但保留日志
                traceback.print_exc()

    def _publish(self, job: Job, candidate: ReconcileResult | None) -> None:
        """原子发布终态：状态与 result 在同一次持锁写入中可见。

        顺序：全部 SQL/读取线程已退出、暂存已清空 -> 释放名额 -> 发布终态。
        裁决规则（事件序号最小者胜出）：
        1) 任一侧真实失败：failed，保留原始失败原因；
        2) 取消先于超时被受理：cancelled，不发布结论；
        3) 超时：timeout，不发布结论；
        4) 无终止事件且已产出结论：succeeded；
        5) 其他：failed（未产出结论）。
        """
        with job._lock:
            if job.status in TERMINAL_STATUSES:
                return

            if job.failure_seq is not None and job.failure is not None:
                status = "failed"
                error = self._format_failure(job.failure)
                result = None
            elif self._cancel_is_first(job):
                status = "cancelled"
                error = "本轮已取消，未发布比较结论"
                result = None
            elif job.timeout_seq is not None:
                status = "timeout"
                error = (
                    f"超过单轮 {settings.job_timeout_seconds} 秒时限，"
                    f"已终止本轮全部 SQL，未发布比较结论"
                )
                result = None
            elif candidate is not None:
                status = "succeeded"
                error = None
                result = candidate
            else:
                status = "failed"
                error = "内部错误：未产出比较结论"
                result = None

            # 状态、结论、finished_at、名额释放全部在同一临界区内完成：
            # 查询方要么看到“运行中且无结论”，要么看到“终态 + 对应结论”，
            # 且看到终态时名额必然已经释放；新建任务也不可能插进中间态。
            with self._slot_lock:
                job.error = error
                job.result = result
                job.finished_at = _utcnow()
                job.status = status

                if self._active is job:
                    self._active = None
                self._last_terminal = job
                if len(self._jobs) > 20:
                    for jid in list(self._jobs):
                        other = self._jobs[jid]
                        if jid != job.job_id and other.status in TERMINAL_STATUSES:
                            self._jobs.pop(jid, None)
                            break

    @staticmethod
    def _cancel_is_first(job: Job) -> bool:
        cancel_active = job.cancel_seq is not None or job.cancel_requested
        if not cancel_active:
            return False
        if job.timeout_seq is None:
            return True
        # cancel_seq 缺失只可能来自外部直接置标志；此时不臆断它早于超时
        return job.cancel_seq is not None and job.cancel_seq < job.timeout_seq

    @staticmethod
    def _format_failure(failure: tuple[str | None, BaseException]) -> str:
        side, exc = failure
        if side is None:
            if isinstance(exc, dbmod.DbError):
                return str(exc)
            return f"内部错误：{exc.__class__.__name__}: {exc}"
        return f"{side} 侧查询失败：{exc}"


class SlotBusyError(RuntimeError):
    def __init__(self, active_job_id: str):
        super().__init__(f"已有核对任务在运行：job_id={active_job_id}")
        self.active_job_id = active_job_id


# 模块级单例
manager = JobManager()
