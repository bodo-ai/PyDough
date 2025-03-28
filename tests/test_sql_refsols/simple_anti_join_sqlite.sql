WITH _table_alias_1 AS (
  SELECT
    table.a AS a,
    table.b AS b
  FROM table AS table
)
SELECT
  _table_alias_0.a AS a
FROM _table_alias_1 AS _table_alias_0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _table_alias_1 AS _table_alias_1
    WHERE
      _table_alias_0.a = _table_alias_1.a
  )
