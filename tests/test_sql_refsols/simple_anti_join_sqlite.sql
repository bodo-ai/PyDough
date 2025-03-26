WITH _table_alias_0 AS (
  SELECT
    table.a AS a,
    table.b AS b
  FROM table AS table
), _u_0 AS (
  SELECT
    _table_alias_1.a AS _u_1
  FROM _table_alias_0 AS _table_alias_1
  GROUP BY
    _table_alias_1.a
)
SELECT
  _table_alias_0.a AS a
FROM _table_alias_0 AS _table_alias_0
LEFT JOIN _u_0 AS _u_0
  ON _table_alias_0.a = _u_0._u_1
WHERE
  _u_0._u_1 IS NULL
