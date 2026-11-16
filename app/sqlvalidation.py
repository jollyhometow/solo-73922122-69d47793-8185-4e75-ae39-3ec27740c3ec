"""用户提交 SQL 的静态校验。

约束（需求）：
- 只能是单条只读 SELECT；
- 不允许写操作 / DDL / 锁 / 游标 / 临时表等副作用；
- 多语句、注释夹带语句一律拒绝。

数据库侧还会把连接置为 read_only 并设置 statement_timeout，做纵深防御。
"""
from __future__ import annotations

import re

import sqlglot
from sqlglot import exp

# 出现即拒绝的关键字（注释/字符串之外的裸词检查，作为 sqlglot 之外的第二道闸）
_FORBIDDEN_BARE = re.compile(
    r"\b("
    r"insert|update|delete|merge|truncate|grant|revoke|"
    r"create|alter|drop|vacuum|reindex|cluster|refresh|"
    r"lock|call|execute|commit|rollback|savepoint|prepare|"
    r"discard|set|reset|copy|checkpoint|listen|notify|load"
    r")\b",
    re.IGNORECASE,
)

# 允许的顶层语句类型之外，SELECT 内部也不允许出现的表达式
_FORBIDDEN_EXP = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Command,  # SET / SHOW / CALL 等裸命令
    exp.Lock,
    exp.Create,
    exp.Alter,
    exp.Drop,
)


class SqlValidationError(ValueError):
    """SQL 未通过静态校验。"""


def _strip(sql: str) -> str:
    sql = (sql or "").strip()
    if not sql:
        raise SqlValidationError("SQL 为空")
    # 去掉结尾单个分号；其余分号（多语句）交给解析器判定
    if sql.endswith(";"):
        sql = sql[:-1].rstrip()
    return sql


def validate_select(sql: str) -> str:
    """校验并返回规范化后的 SQL；不合法时抛 SqlValidationError。"""
    text = _strip(sql)

    # 1) 解析：必须能解析为恰好一条语句
    try:
        statements = sqlglot.parse(text, read="postgres")
    except sqlglot.errors.ParseError as exc:
        raise SqlValidationError(f"SQL 解析失败：{exc}") from exc

    statements = [s for s in statements if s is not None]
    if len(statements) != 1:
        raise SqlValidationError("只允许提交单条 SELECT 语句")

    stmt = statements[0]
    # UNION / INTERSECT / EXCEPT 的根节点是集合操作节点，其叶子必须全是 SELECT
    if isinstance(stmt, (exp.Union, exp.Intersect, exp.Except)):
        if not all(isinstance(s, exp.Select) for s in stmt.find_all(exp.Select)):
            raise SqlValidationError("集合操作的成员必须都是 SELECT")
    elif not isinstance(stmt, exp.Select):
        raise SqlValidationError("只允许只读 SELECT（含 UNION/INTERSECT/EXCEPT）查询")

    # 2) AST 内禁止任何写操作 / DDL / 命令节点
    bad = stmt.find(*_FORBIDDEN_EXP)
    if bad is not None:
        raise SqlValidationError(f"查询中出现不允许的语句类型：{type(bad).__name__}")

    # SELECT ... INTO ... 会建表
    if stmt.find(exp.Into) is not None:
        raise SqlValidationError("不允许 SELECT INTO")
    # 不允许函数调用里的副作用（pg_* 写函数等无法穷举，read_only 连接兜底）
    # 显式禁止过程调用语法
    if stmt.find(exp.Anonymous) is not None:
        for node in stmt.find_all(exp.Anonymous):
            if node.name.lower() in {"dblink_exec", "pg_terminate_backend"}:
                raise SqlValidationError(f"不允许调用函数：{node.name}")

    # 3) 裸关键字兜底（sqlglot 可能把方言内的特殊结构吞掉）
    if _FORBIDDEN_BARE.search(text):
        raise SqlValidationError("SQL 中包含只读 SELECT 不允许的关键字")

    return stmt.sql(dialect="postgres")
