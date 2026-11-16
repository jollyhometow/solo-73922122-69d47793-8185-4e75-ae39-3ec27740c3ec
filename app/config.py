"""运行期配置（全部可通过环境变量覆盖）。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except ValueError:
        return default


@dataclass
class Settings:
    # 数据库连接
    pg_dsn: str = _env(
        "PG_DSN",
        "postgresql://recon:recon@postgres:5432/recon",
    )
    connect_timeout: int = _env_int("PG_CONNECT_TIMEOUT", 5)
    statement_timeout_ms: int = _env_int("PG_STATEMENT_TIMEOUT_MS", 28000)
    lock_timeout_ms: int = _env_int("PG_LOCK_TIMEOUT_MS", 2000)
    idle_in_tx_timeout_ms: int = _env_int("PG_IDLE_IN_TX_TIMEOUT_MS", 60000)

    # 每轮核对的硬性时限（秒）
    job_timeout_seconds: int = _env_int("JOB_TIMEOUT_SECONDS", 30)

    # 流式归并的内存/磁盘参数
    chunk_rows: int = _env_int("SORT_CHUNK_ROWS", 20000)
    max_diff_types: int = _env_int("MAX_DIFF_TYPES", 20)

    # 暂存目录（容器内），独立挂载 tmpfs / 卷
    work_dir: str = _env("WORK_DIR", "/tmp/recon-work")

    # 允许出现在结果集中的列类型（PostgreSQL 类型名 -> psycopg 加载后的 Python 类型）
    allowed_pg_types: tuple[str, ...] = ("bigint", "text")


settings = Settings()
