"""50 万行级端到端压测：内存有界性、暂存落盘、结果正确性。

直接驱动 JobManager（绕过 HTTP），用 resource 监控进程 RSS 峰值，
并统计暂存目录的磁盘占用。两侧故意制造一处重复计入差异。
"""
from __future__ import annotations

import os
import resource
import sys
import time

sys.path.insert(0, "/workspace")
os.environ.setdefault("PG_DSN", "postgresql://postgres@/postgres?host=/tmp/pgdata")
os.environ["WORK_DIR"] = "/tmp/recon-load-work"
os.environ["SORT_CHUNK_ROWS"] = "20000"
os.environ["JOB_TIMEOUT_SECONDS"] = "30"

from app.manager import TERMINAL_STATUSES, JobManager  # noqa: E402


def rss_mb() -> float:
    # ru_maxrss 在 Linux 上单位为 KB
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def dir_size(path: str) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def main() -> int:
    base = (
        "SELECT meter_id::bigint AS meter_id, read_time::text AS read_time, "
        "wh::bigint AS wh FROM readings"
    )
    # 左侧追加一条已存在行的完全副本，制造一次重复计入（括号隔离 LIMIT）
    dup = (
        "SELECT * FROM (SELECT meter_id::bigint, read_time::text, wh::bigint "
        "FROM readings LIMIT 1) AS one_row"
    )
    left_sql = f"{base} UNION ALL {dup}"

    m = JobManager()
    t0 = time.time()
    job = m.create_job(left_sql=left_sql, right_sql=base, name="load-500k")
    peak = rss_mb()
    peak_disk = 0
    while job.status not in TERMINAL_STATUSES:
        time.sleep(0.2)
        peak = max(peak, rss_mb())
        if job.work_dir:
            peak_disk = max(peak_disk, dir_size(job.work_dir))
    elapsed = time.time() - t0

    print(f"status        : {job.status}")
    print(f"elapsed       : {elapsed:.1f}s (limit 30s)")
    print(f"peak RSS      : {peak:.1f} MiB (container limit 128 MiB)")
    print(f"peak staging  : {peak_disk / 1024 / 1024:.1f} MiB")
    if job.error:
        print(f"error         : {job.error}")
    if job.result:
        r = job.result
        print(f"left rows     : {r.left_row_total}")
        print(f"right rows    : {r.right_row_total}")
        print(f"identical     : {r.identical}")
        print(f"diff groups   : {r.diff_group_total} (shown {r.diffs_shown})")
        print(f"column types  : {r.column_types}")
        print(f"sample diff   : {r.diffs[0] if r.diffs else None}")

    # 暂存必须已清空
    leftover = os.path.exists(job.work_dir) if job.work_dir else False
    print(f"staging left  : {leftover}")
    print(f"slot freed    : {m._active is None}")

    ok = (
        job.status == "succeeded"
        and job.result is not None
        and job.result.identical is False
        and job.result.diff_group_total == 1
        and job.result.left_row_total == job.result.right_row_total + 1
        and not leftover
        and m._active is None
        and peak < 128
    )
    print("RESULT        :", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
