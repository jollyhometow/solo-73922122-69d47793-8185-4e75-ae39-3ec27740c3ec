"""PostgreSQL 访问层。

关键保证：
- 三条独立连接（快照连接 + 左/右读取连接），两条读取连接通过
  pg_export_snapshot / SET TRANSACTION SNAPSHOT 绑定到同一个已提交快照，
  核对期间源表仍可被其他会话更正，不影响本轮看到的数据；
- 读取连接全部 READ ONLY + REPEATABLE READ，并设 statement_timeout；
- 使用服务端命名游标分批 fetch，避免客户端整表缓存；
- 支持从其他线程调用 conn.cancel() 及时中止正在执行的 SQL。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

import psycopg

from .config import settings

# PostgreSQL OID -> 规范类型名（结果列只接受这两种）
_OID_TO_TYPE = {
    20: "bigint",  # int8
    25: "text",
}


class DbError(RuntimeError):
    pass


@dataclass
class ColumnMeta:
    name: str
    pg_type: str  # "bigint" | "text"


def _open_connection() -> psycopg.Connection:
    conn = psycopg.connect(
        settings.pg_dsn,
        connect_timeout=settings.connect_timeout,
        autocommit=True,          # 事务边界自己用 BEGIN 显式控制
        application_name="recon-api",
    )
    # SET 不接受绑定参数，以下均为内部 int 配置，直接内联安全
    conn.execute(f"SET SESSION statement_timeout = {int(settings.statement_timeout_ms)}")
    conn.execute(f"SET SESSION lock_timeout = {int(settings.lock_timeout_ms)}")
    conn.execute(
        f"SET SESSION idle_in_transaction_session_timeout = "
        f"{int(settings.idle_in_tx_timeout_ms)}"
    )
    # 单连接流式拉取即可，禁用并行 gather 以稳定内存与暂存行为
    conn.execute("SET SESSION max_parallel_workers_per_gather = 0")
    return conn


class SnapshotSession:
    """管理同一快照下的两条只读读取连接。"""

    def __init__(self) -> None:
        self._snapshot_conn: psycopg.Connection | None = None
        self.readers: list[psycopg.Connection] = []
        self.snapshot_id: str | None = None
        self._closed = False
        self._lock = threading.Lock()

    def open(self) -> None:
        # 快照连接：开启可重复读只读事务并导出快照
        snap_conn = _open_connection()
        try:
            snap_conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            row = snap_conn.execute("SELECT pg_export_snapshot()").fetchone()
            self.snapshot_id = row[0] if row else None
            if not self.snapshot_id:
                raise DbError("pg_export_snapshot() 未返回快照标识")
            self._snapshot_conn = snap_conn

            # 两条独立读取连接，绑定同一快照
            for _ in range(2):
                conn = _open_connection()
                conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                # 快照标识来自 pg_export_snapshot，用 Literal 安全内联
                conn.execute(
                    psycopg.sql.SQL("SET TRANSACTION SNAPSHOT {}").format(
                        psycopg.sql.Literal(self.snapshot_id)
                    )
                )
                self.readers.append(conn)
        except Exception:
            self.close()
            raise

    def cancel_all(self) -> None:
        """请求取消所有连接上正在执行的 SQL（可从控制线程调用）。"""
        with self._lock:
            targets: list[psycopg.Connection] = []
            if self._snapshot_conn is not None:
                targets.append(self._snapshot_conn)
            targets.extend(self.readers)
        for conn in targets:
            try:
                conn.cancel()
            except Exception:
                # 连接可能已断开；终止流程由 close/terminate 兜底
                pass

    def terminate_backends(self) -> None:
        """cancel 不生效时的兜底：通过管理员连接 pg_terminate_backend。"""
        if not self.readers:
            return
        pids: list[int] = []
        for conn in list(self.readers):
            try:
                pids.append(conn.info.backend_pid)
            except Exception:
                pass
        if not pids:
            return
        try:
            admin = _open_connection()
            try:
                admin.execute(
                    "SELECT pg_terminate_backend(pid) FROM unnest(%s::int[]) AS pid",
                    (pids,),
                )
            finally:
                admin.close()
        except Exception:
            pass

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            for conn in self.readers:
                _quiet_close(conn)
            if self._snapshot_conn is not None:
                _quiet_close(self._snapshot_conn)
            self.readers = []
            self._snapshot_conn = None


def _quiet_close(conn: psycopg.Connection) -> None:
    try:
        conn.close()
    except Exception:
        pass


def describe_and_stream(
    conn: psycopg.Connection,
    sql: str,
    cursor_name: str,
    work_dir: str,
    chunk_rows: int,
    is_cancelled,
    deadline_check,
) -> tuple[list[ColumnMeta], list[str], int]:
    """执行 SQL，校验结果列类型，并把结果流式写成外部排序 run 文件。

    返回 (列元数据, run 文件路径列表, 拉取总行数)。
    使用命名服务端游标：psycopg 仅在 fetchmany 时按 itersize 传输。
    """
    from .external_merge import RunWriter  # 避免循环导入

    writer = RunWriter(work_dir=work_dir, chunk_rows=chunk_rows)
    columns: list[ColumnMeta] = []
    total = 0
    # itersize 控制每次网络往返的行数，内存里同时只有一批
    cur = conn.cursor(name=cursor_name, binary=False, withhold=False)
    cur.itersize = min(chunk_rows, 10000)
    try:
        # 执行前检查点：若取消在查询派发前到达，直接结束，不向服务器发 SQL
        if is_cancelled():
            from .external_merge import JobCancelled

            raise JobCancelled()
        deadline_check()
        cur.execute(sql)  # 命名游标上不向客户端缓存全部结果
        if cur.description is None:
            raise DbError("查询没有返回结果集，请确认提交的是 SELECT")
        for desc in cur.description:
            pg_type = _OID_TO_TYPE.get(desc.type_code)
            if pg_type is None:
                raise DbError(
                    f"结果列 {desc.name!r} 的类型 OID={desc.type_code} 不受支持，"
                    f"只允许 bigint/text"
                )
            columns.append(ColumnMeta(name=desc.name, pg_type=pg_type))

        batch: list[tuple[Any, ...]] = []
        while True:
            if is_cancelled():
                from .external_merge import JobCancelled

                raise JobCancelled()
            deadline_check()
            rows = cur.fetchmany(cur.itersize)
            if not rows:
                break
            _validate_row_types(rows, columns)
            batch.extend(rows)
            total += len(rows)
            if len(batch) >= chunk_rows:
                writer.add(batch)
                batch = []
        if batch:
            writer.add(batch)
        runs = writer.finish()
        return columns, runs, total
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _validate_row_types(rows: list[tuple], columns: list[ColumnMeta]) -> None:
    """防御性校验：实际 Python 类型必须与声明的 bigint/text 一致。"""
    for row in rows:
        if len(row) != len(columns):
            raise DbError("结果行列数与描述不一致")
        for value, meta in zip(row, columns):
            if value is None:
                continue
            if meta.pg_type == "bigint" and not isinstance(value, int):
                raise DbError(f"列 {meta.name!r} 期望 bigint，实际值 {value!r}")
            if meta.pg_type == "text" and not isinstance(value, str):
                raise DbError(f"列 {meta.name!r} 期望 text，实际值 {value!r}")
