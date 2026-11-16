"""external_merge 的纯逻辑测试（不依赖数据库）。"""
from __future__ import annotations

import tempfile

import pytest

from app.external_merge import (
    JobCancelled,
    RunWriter,
    iter_sorted,
    reconcile_sorted,
    sort_key,
)


def _write_runs(work_dir: str, rows: list[tuple], chunk: int) -> list[str]:
    w = RunWriter(work_dir=work_dir, chunk_rows=chunk)
    for i in range(0, len(rows), max(1, chunk // 2)):
        w.add(rows[i : i + max(1, chunk // 2)])
    return w.finish()


def test_sort_key_null_before_empty_string_before_text() -> None:
    assert sort_key((None,)) < sort_key(("",))
    assert sort_key(("",)) < sort_key(("a",))
    assert sort_key((1,)) < sort_key(("1",))
    assert sort_key((None, 0)) < sort_key((1, 0))


def test_null_and_empty_string_are_different_groups() -> None:
    with tempfile.TemporaryDirectory() as d1, tempfile.TemporaryDirectory() as d2:
        # 同列位置：None 与 "" 必须被视为不同的行内容
        left = [(1, None), (2, "")]
        right = [(1, ""), (2, None)]
        lr = _write_runs(d1, left, 2)
        rr = _write_runs(d2, right, 2)
        stats = reconcile_sorted(
            lr, rr, left_total=2, right_total=2,
            max_diff_types=20, is_cancelled=lambda: False,
        )
        # 4 个等值组互不相同，每个组仅单侧存在
        assert stats.diff_group_total == 4
        assert stats.matched_group_count == 0
        contents = {tuple(x["values"]) for x in stats.diffs}
        assert contents == {(1, None), (2, ""), (1, ""), (2, None)}


def test_null_vs_empty_same_key_column_duplicate_counts() -> None:
    """两侧行内容相同位置上 NULL 与空串的次数分别统计，不会被合并。"""
    with tempfile.TemporaryDirectory() as d1, tempfile.TemporaryDirectory() as d2:
        left = [(1, None), (1, None), (1, "")]
        right = [(1, None), (1, ""), (1, "")]
        lr = _write_runs(d1, left, 2)
        rr = _write_runs(d2, right, 2)
        stats = reconcile_sorted(
            lr, rr, left_total=3, right_total=3,
            max_diff_types=20, is_cancelled=lambda: False,
        )
        assert stats.diff_group_total == 2
        by_value = {tuple(d["values"]): d for d in stats.diffs}
        assert by_value[(1, None)]["left_count"] == 2
        assert by_value[(1, None)]["right_count"] == 1
        assert by_value[(1, "")]["left_count"] == 1
        assert by_value[(1, "")]["right_count"] == 2


def test_duplicates_counted_as_multiset_order_ignored() -> None:
    with tempfile.TemporaryDirectory() as d1, tempfile.TemporaryDirectory() as d2:
        left = [(1, "a"), (1, "a"), (2, "b"), (3, "c")]
        right = [(3, "c"), (2, "b"), (1, "a")]  # 左侧 (1,a) 重复计入
        lr = _write_runs(d1, left, 2)
        rr = _write_runs(d2, right, 2)
        stats = reconcile_sorted(
            lr, rr, left_total=4, right_total=3,
            max_diff_types=20, is_cancelled=lambda: False,
        )
        assert stats.matched_group_count == 2
        assert stats.diff_group_total == 1
        diff = stats.diffs[0]
        assert diff["values"] == [1, "a"]
        assert diff["left_count"] == 2 and diff["right_count"] == 1


def test_identical_multisets_with_multiple_runs() -> None:
    with tempfile.TemporaryDirectory() as d1, tempfile.TemporaryDirectory() as d2:
        rows = [(i, f"m{i % 3}") for i in range(1000)]
        lr = _write_runs(d1, rows, 37)   # 制造很多 run，强制 k 路归并
        rr = _write_runs(d2, list(reversed(rows)), 37)
        stats = reconcile_sorted(
            lr, rr, left_total=1000, right_total=1000,
            max_diff_types=20, is_cancelled=lambda: False,
        )
        assert stats.diff_group_total == 0
        assert stats.matched_group_count == 1000
        # 排序结果可复现：iter_sorted 产出 (键, 行)，取行后与基准排序一致
        merged = [row for _key, row in iter_sorted(lr)]
        assert merged == sorted(rows, key=sort_key)


def test_diff_display_cap_keeps_full_counts() -> None:
    with tempfile.TemporaryDirectory() as d1, tempfile.TemporaryDirectory() as d2:
        left = [(i, "x") for i in range(50)]
        right: list[tuple] = []
        lr = _write_runs(d1, left, 7)
        rr = _write_runs(d2, right, 7)
        stats = reconcile_sorted(
            lr, rr, left_total=50, right_total=0,
            max_diff_types=20, is_cancelled=lambda: False,
        )
        assert stats.diff_group_total == 50       # 全量计数
        assert len(stats.diffs) == 20             # 展示最多 20 种


def test_cancel_during_reconcile_raises() -> None:
    with tempfile.TemporaryDirectory() as d1, tempfile.TemporaryDirectory() as d2:
        lr = _write_runs(d1, [(i, "a") for i in range(100)], 5)
        rr = _write_runs(d2, [(-i, "b") for i in range(100)], 5)
        with pytest.raises(JobCancelled):
            reconcile_sorted(
                lr, rr, left_total=100, right_total=100,
                max_diff_types=20, is_cancelled=lambda: True,
            )


def test_empty_result_sets_are_identical() -> None:
    with tempfile.TemporaryDirectory() as d1, tempfile.TemporaryDirectory() as d2:
        lr = _write_runs(d1, [], 10)
        rr = _write_runs(d2, [], 10)
        stats = reconcile_sorted(
            lr, rr, left_total=0, right_total=0,
            max_diff_types=20, is_cancelled=lambda: False,
        )
        assert stats.diff_group_total == 0
        assert stats.diffs == []
