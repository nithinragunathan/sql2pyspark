# sql2pyspark

A library that transpiles SQL `SELECT` statements into idiomatic PySpark DataFrame API code.

## Installation

```bash
pip install sql2pyspark
```

Requires Python 3.8+ and `sqlglot>=20`.

## Usage

```python
from sql2pyspark import transpile

print(transpile("SELECT name, age FROM users WHERE age > 30 ORDER BY age DESC LIMIT 10"))
# users.filter((col("age") > 30)).select(col("name"), col("age")).orderBy(col("age").desc()).limit(10)

print(transpile("SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM employees GROUP BY dept"))
# employees.groupBy(col("dept")).agg(count("*").alias("cnt"), avg(col("salary")).alias("avg_sal"))
```

Pass `df_name` to control the source DataFrame variable name:

```python
transpile("SELECT id FROM events WHERE ts > 1000", df_name="events_df")
# events_df.filter((col("ts") > 1000)).select(col("id"))
```

## Supported SQL constructs

| Construct | PySpark output |
|---|---|
| `SELECT col1, col2` | `.select(col("col1"), col("col2"))` |
| `SELECT *` | *(no `.select()` call)* |
| `FROM table` | `table` as the base DataFrame |
| `WHERE expr` | `.filter(expr)` |
| `GROUP BY col` | `.groupBy(col("col"))` |
| `COUNT(*) / SUM / AVG / MAX / MIN` | `.agg(count("*"), sum(...), ...)` |
| `ORDER BY col` / `ORDER BY col DESC` | `.orderBy(col("col"))` / `.orderBy(col("col").desc())` |
| `LIMIT n` | `.limit(n)` |
| `AS alias` | `.alias("alias")` |
| `AND` / `OR` / `NOT` | `&` / `\|` / `~` |
| `=`, `!=`, `>`, `>=`, `<`, `<=` | `==`, `!=`, `>`, `>=`, `<`, `<=` |
| `+`, `-`, `*`, `/` | arithmetic operators |

## Generated code usage

The generated expression assumes PySpark functions are imported:

```python
from pyspark.sql.functions import col, count, sum, avg, max, min
```
