"""生成大规模压测数据（默认约 50 万行 readings）。

用法（容器内或本地 venv，需先建好 sql/init.sql 中的源表）：
    python scripts/seed_large.py --meters 200 --days 105
生成量 = meters * 24 * days，默认 200*24*105 ≈ 50.4 万行。

只做 INSERT，源表随时可被更正（DELETE/UPDATE 后重跑即可）。
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone

import psycopg

DSN = os.environ.get("PG_DSN", "postgresql://postgres@/postgres?host=/tmp/pgdata")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--meters", type=int, default=200)
    parser.add_argument("--days", type=int, default=105)
    parser.add_argument("--batch", type=int, default=5000)
    args = parser.parse_args()

    start_day = datetime(2026, 8, 1, tzinfo=timezone.utc)
    conn = psycopg.connect(DSN, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO meters (meter_id, line_name) VALUES (%s, %s) "
                "ON CONFLICT DO NOTHING",
                [
                    (5000 + i, f"产线{(i % 10) + 1}")
                    for i in range(args.meters)
                ],
            )

            total = 0
            batch: list[tuple] = []
            for i in range(args.meters):
                meter_id = 5000 + i
                wh = 1000
                for d in range(args.days):
                    for h in range(24):
                        wh += (i * 7 + h * 13 + d) % 97
                        ts = start_day + timedelta(days=d, hours=h)
                        batch.append((meter_id, ts, wh))
                        if len(batch) >= args.batch:
                            _flush(cur, batch)
                            total += len(batch)
                            batch.clear()
            if batch:
                _flush(cur, batch)
                total += len(batch)
            print(f"inserted readings: {total}")
    finally:
        conn.close()


def _flush(cur, batch: list[tuple]) -> None:
    cur.executemany(
        "INSERT INTO readings (meter_id, read_time, wh) "
        "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
        batch,
    )


if __name__ == "__main__":
    main()
