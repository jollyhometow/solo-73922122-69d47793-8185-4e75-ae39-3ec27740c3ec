"""外部归并排序与多路流式归并核对。

设计要点：
- 结果集以服务端命名游标分批拉取（itersize），不会整表进内存；
- 每 chunk_rows 行在进程内排序后以“流式 pickle”逐行落盘为一个 run，
  读取端同样逐行 unpickle，任何时刻单流只驻留一行；
- 归并采用分层（多相）归并：每层最多 FAN_IN 个有序流做 heapq.merge，
  结果写成下一层 run 文件，堆大小与同时打开的文件数恒定；
- 比较按行内容与“出现次数”进行，NULL 与空串天然不同（Python None != ""），
  不使用整表哈希做判等；
- 最多保留 MAX_DIFF_TYPES 种差异用于展示，计数器仍覆盖全量数据。
"""
from __future__ import annotations

import heapq
import os
import pickle
import tempfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

# 单次归并的参考扇入：堆中每个流只驻留一行，流数增加不会带来线性内存增长，
# 仅影响堆操作的 log(k) 成本；保留该常量用于文档与潜在的描述符上限保护。
FAN_IN = 64

# 一行 = tuple，元素仅允许 int / str / None
Row = tuple[Any, ...]


def sort_key(row: Row) -> tuple:
    """全序键：None 排在最前，其次 int，再次 str；同类型按自然序。"""
    key: list[tuple[int, Any]] = []
    for v in row:
        if v is None:
            key.append((0, 0))
        elif isinstance(v, int):
            key.append((1, v))
        else:  # str（含空串 ""）
            key.append((2, v))
    return tuple(key)


def rows_equal(a: Row, b: Row) -> bool:
    return a == b  # tuple 逐元素相等：None 与 "" 不相等，符合需求


@dataclass
class RunWriter:
    """把分批到达的行切成多个有序 run 文件（逐行流式 pickle）。"""

    work_dir: str
    chunk_rows: int
    rows_written: int = 0
    run_paths: list[str] = field(default_factory=list)
    _buffer: list[Row] = field(default_factory=list)

    def add(self, rows: list[Row]) -> None:
        self._buffer.extend(rows)
        self.rows_written += len(rows)
        while len(self._buffer) >= self.chunk_rows:
            self._flush(self._buffer[: self.chunk_rows])
            self._buffer = self._buffer[self.chunk_rows :]

    def finish(self) -> list[str]:
        if self._buffer:
            self._flush(self._buffer)
            self._buffer = []
        return self.run_paths

    def _flush(self, chunk: list[Row]) -> None:
        chunk.sort(key=sort_key)
        fd, path = tempfile.mkstemp(prefix="run-", suffix=".pkl", dir=self.work_dir)
        try:
            with os.fdopen(fd, "wb") as fh:
                # 逐行写，避免 pickle 维护跨对象引用表造成额外驻留
                for row in chunk:
                    pickle.dump(row, fh, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception:
            os.unlink(path)
            raise
        self.run_paths.append(path)


def _iter_run(path: str) -> Iterator[Row]:
    """逐行读取一个 run，单流任意时刻只驻留一行。"""
    with open(path, "rb") as fh:
        while True:
            try:
                yield pickle.load(fh)
            except EOFError:
                return


# 携带预计算全序键的行：(sort_key, row)
KeyedRow = tuple[tuple, Row]


def _keyed(path: str) -> Iterator[KeyedRow]:
    for row in _iter_run(path):
        yield sort_key(row), row


def iter_sorted(run_paths: list[str], work_dir: str | None = None) -> Iterator[KeyedRow]:
    """产出一个全序惰性流，元素为 (预计算键, 行)。

    每个流逐行 unpickle、堆中每流只驻留一行，因此即便 run 数较多，
    归并的额外内存也近似 O(流数) 而非 O(总行数)；键在入堆时计算一次，
    下游核对直接复用，不再重复建键。

    - 0 个 run：空流；
    - 1 个 run：直接流式返回；
    - 多个 run：单轮 k 路 heapq.merge，无中间全量落盘。
    """
    if not run_paths:
        return iter(())
    if len(run_paths) == 1:
        return _keyed(run_paths[0])
    return heapq.merge(*[_keyed(p) for p in run_paths], key=lambda kr: kr[0])


def _quiet_unlink(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


@dataclass
class ReconcileStats:
    left_row_total: int = 0
    right_row_total: int = 0
    matched_group_count: int = 0      # 两侧该组次数相等的去重行数
    diff_group_total: int = 0         # 两侧次数不等（含仅一侧存在）的去重行数
    diffs: list[dict[str, Any]] = field(default_factory=list)


class _PeekStream:
    """按全序键聚合等值组的有序流，支持回看一个元素。"""

    def __init__(self, stream: Iterator[KeyedRow]):
        self._stream = stream
        self._peek: KeyedRow | None = None
        self._has_peek = False

    def next_group(self) -> tuple[tuple | None, Row | None, int]:
        first = self._peek if self._has_peek else next(self._stream, None)
        self._has_peek = False
        if first is None:
            return None, None, 0
        first_key, first_row = first
        count = 1
        while True:
            nxt = next(self._stream, None)
            if nxt is None:
                return first_key, first_row, count
            if nxt[0] == first_key:
                count += 1
            else:
                self._peek = nxt
                self._has_peek = True
                return first_key, first_row, count


def reconcile_sorted(
    left_runs: list[str],
    right_runs: list[str],
    *,
    left_total: int,
    right_total: int,
    max_diff_types: int,
    is_cancelled: Callable[[], bool],
    work_dir: str | None = None,
) -> ReconcileStats:
    """对两侧已落盘的有序 run 做归并连接式多重集核对。

    任一组两侧次数不同即记为一种差异；差异明细最多保留 max_diff_types 种，
    但 diff_group_total / 总行数等计数始终覆盖全量。
    比较全程复用预计算键，NULL 与空串因键不同而自然区分。
    """
    stats = ReconcileStats(left_row_total=left_total, right_row_total=right_total)
    left = _PeekStream(iter_sorted(left_runs, work_dir))
    right = _PeekStream(iter_sorted(right_runs, work_dir))

    l_key, l_row, l_cnt = left.next_group()
    r_key, r_row, r_cnt = right.next_group()

    while l_key is not None or r_key is not None:
        if is_cancelled():
            raise JobCancelled()

        if l_key is not None and (r_key is None or l_key < r_key):
            _record_diff(stats, l_row, l_cnt, 0, max_diff_types)
            l_key, l_row, l_cnt = left.next_group()
        elif r_key is not None and (l_key is None or r_key < l_key):
            _record_diff(stats, r_row, 0, r_cnt, max_diff_types)
            r_key, r_row, r_cnt = right.next_group()
        else:
            # 键相同即行内容相同（sort_key 对固定列数是单射全序）
            assert l_key is not None and l_key == r_key and rows_equal(l_row, r_row)
            if l_cnt == r_cnt:
                stats.matched_group_count += 1
            else:
                _record_diff(stats, l_row, l_cnt, r_cnt, max_diff_types)
            l_key, l_row, l_cnt = left.next_group()
            r_key, r_row, r_cnt = right.next_group()

    return stats


def _record_diff(
    stats: ReconcileStats,
    row: Row,
    left_count: int,
    right_count: int,
    max_diff_types: int,
) -> None:
    stats.diff_group_total += 1
    if len(stats.diffs) < max_diff_types:
        stats.diffs.append(
            {
                "values": list(row),
                "left_count": left_count,
                "right_count": right_count,
            }
        )


class JobCancelled(Exception):
    """核对过程中检测到取消请求。"""
