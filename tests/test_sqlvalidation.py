"""SQL 静态校验测试。"""
from __future__ import annotations

import pytest

from app.sqlvalidation import SqlValidationError, validate_select


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1::bigint AS a, 'x'::text AS b",
        "select meter_id from readings where wh > 10",
        "SELECT * FROM readings UNION SELECT * FROM readings",
        "SELECT line_name, count(*)::bigint FROM meters GROUP BY 1;",
        "  SELECT 1  ;  ",
        """
        WITH d AS (SELECT meter_id, wh FROM readings)
        SELECT meter_id, SUM(wh)::bigint FROM d GROUP BY meter_id
        """,
    ],
)
def test_valid_selects_pass(sql: str) -> None:
    assert validate_select(sql).upper().startswith("SELECT") or "SELECT" in validate_select(sql).upper()


@pytest.mark.parametrize(
    "sql",
    [
        "",
        "SELECT 1; DROP TABLE readings;",
        "DELETE FROM readings WHERE meter_id = 1",
        "UPDATE meters SET line_name = 'x' WHERE meter_id = 1",
        "INSERT INTO meters VALUES (1, 'x')",
        "CREATE TABLE t (a bigint)",
        "ALTER TABLE meters ADD COLUMN c text",
        "TRUNCATE readings",
        "SELECT * INTO new_t FROM readings",
        "LOCK TABLE readings IN ACCESS EXCLUSIVE MODE",
        "CALL some_proc()",
        "SET statement_timeout = 0; SELECT 1",
        "COPY readings TO '/tmp/x.csv'",
        "WITH x AS (DELETE FROM readings RETURNING *) SELECT * FROM x",
        "GRANT SELECT ON readings TO recon",
    ],
)
def test_illegal_sql_rejected(sql: str) -> None:
    with pytest.raises(SqlValidationError):
        validate_select(sql)


def test_semicolon_only_is_rejected() -> None:
    with pytest.raises(SqlValidationError):
        validate_select(";")
