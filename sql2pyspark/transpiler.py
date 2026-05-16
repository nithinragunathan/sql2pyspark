"""SQL to PySpark DataFrame API transpiler."""

from typing import Optional

import sqlglot
from sqlglot import exp


def transpile(sql: str, df_name: Optional[str] = None) -> str:
    """Transpile a SQL SELECT statement into PySpark DataFrame API code.

    Args:
        sql: SQL string to transpile.
        df_name: Variable name to use for the source DataFrame. Defaults to
            the table name in the FROM clause, or "df" if no table is found.

    Returns:
        A string containing the PySpark fluent-chain expression.

    Raises:
        ValueError: If the SQL is not a SELECT statement or contains
            unsupported constructs.
    """
    tree = sqlglot.parse_one(sql)
    if not isinstance(tree, exp.Select):
        raise ValueError(
            f"Only SELECT statements are supported, got: {type(tree).__name__}"
        )
    return _select_to_pyspark(tree, df_name)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _select_to_pyspark(node: exp.Select, df_name: Optional[str]) -> str:
    from_clause = node.find(exp.From)
    if from_clause:
        table = from_clause.find(exp.Table)
        source = df_name or (table.name if table else "df")
    else:
        source = df_name or "df"

    parts = [source]

    where = node.find(exp.Where)
    if where:
        parts.append(f"filter({_expr(where.this)})")

    group = node.find(exp.Group)
    if group:
        group_cols = ", ".join(_col_ref(e) for e in group.expressions)
        parts.append(f"groupBy({group_cols})")

        agg_items = [
            _expr(e) for e in node.expressions if _contains_agg(e)
        ]
        if agg_items:
            parts.append(f"agg({', '.join(agg_items)})")
    else:
        if not (len(node.expressions) == 1 and isinstance(node.expressions[0], exp.Star)):
            cols = ", ".join(_expr(e) for e in node.expressions)
            parts.append(f"select({cols})")

    order = node.find(exp.Order)
    if order:
        order_cols = ", ".join(_ordered(o) for o in order.expressions)
        parts.append(f"orderBy({order_cols})")

    limit = node.find(exp.Limit)
    if limit:
        parts.append(f"limit({limit.args['expression'].this})")

    return ".".join(parts)


def _contains_agg(expr: exp.Expression) -> bool:
    return isinstance(expr, exp.AggFunc) or bool(expr.find(exp.AggFunc))


def _col_ref(expr: exp.Expression) -> str:
    """Return col("name") for a column reference, or a full expression."""
    if isinstance(expr, exp.Column):
        return f'F.col("{expr.name}")'
    return _expr(expr)


def _ordered(expr: exp.Ordered) -> str:
    base = _col_ref(expr.this)
    if expr.args.get("desc"):
        return f"{base}.desc()"
    return base


def _expr(expr: exp.Expression) -> str:  # noqa: C901  (acceptable complexity)
    # Alias
    if isinstance(expr, exp.Alias):
        return f'{_expr(expr.this)}.alias("{expr.alias}")'

    # Star
    if isinstance(expr, exp.Star):
        return '"*"'

    # Column reference
    if isinstance(expr, exp.Column):
        return f'F.col("{expr.name}")'

    # Literals
    if isinstance(expr, exp.Literal):
        if expr.is_string:
            # Preserve original string, escape inner double-quotes
            value = expr.this.replace('"', '\\"')
            return f'"{value}"'
        return expr.this  # numeric literal as-is

    if isinstance(expr, exp.Boolean):
        return "True" if expr.this else "False"

    if isinstance(expr, exp.Null):
        return "None"

    # Aggregate functions
    if isinstance(expr, exp.Count):
        arg = expr.this
        if isinstance(arg, exp.Star):
            return 'F.count("*")'
        return f"F.count({_expr(arg)})"

    if isinstance(expr, exp.Sum):
        return f"F.sum({_expr(expr.this)})"

    if isinstance(expr, exp.Avg):
        return f"F.avg({_expr(expr.this)})"

    if isinstance(expr, exp.Max):
        return f"F.max({_expr(expr.this)})"

    if isinstance(expr, exp.Min):
        return f"F.min({_expr(expr.this)})"

    # Comparison operators
    if isinstance(expr, exp.EQ):
        return f"({_expr(expr.left)} == {_expr(expr.right)})"

    if isinstance(expr, exp.NEQ):
        return f"({_expr(expr.left)} != {_expr(expr.right)})"

    if isinstance(expr, exp.GT):
        return f"({_expr(expr.left)} > {_expr(expr.right)})"

    if isinstance(expr, exp.GTE):
        return f"({_expr(expr.left)} >= {_expr(expr.right)})"

    if isinstance(expr, exp.LT):
        return f"({_expr(expr.left)} < {_expr(expr.right)})"

    if isinstance(expr, exp.LTE):
        return f"({_expr(expr.left)} <= {_expr(expr.right)})"

    # Boolean operators
    if isinstance(expr, exp.And):
        return f"({_expr(expr.left)} & {_expr(expr.right)})"

    if isinstance(expr, exp.Or):
        return f"({_expr(expr.left)} | {_expr(expr.right)})"

    if isinstance(expr, exp.Not):
        return f"~({_expr(expr.this)})"

    # Arithmetic
    if isinstance(expr, exp.Add):
        return f"({_expr(expr.left)} + {_expr(expr.right)})"

    if isinstance(expr, exp.Sub):
        return f"({_expr(expr.left)} - {_expr(expr.right)})"

    if isinstance(expr, exp.Mul):
        return f"({_expr(expr.left)} * {_expr(expr.right)})"

    if isinstance(expr, exp.Div):
        return f"({_expr(expr.left)} / {_expr(expr.right)})"

    raise ValueError(
        f"Unsupported expression type '{type(expr).__name__}': {expr}"
    )
