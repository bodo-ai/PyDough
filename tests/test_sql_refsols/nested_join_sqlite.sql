WITH _table_alias_0 AS (
  SELECT
    table.a AS a,
    table.b AS b
  FROM table AS table
), _table_alias_2 AS (
  SELECT
    _table_alias_0.a AS a,
    _table_alias_1.b AS b
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_0 AS _table_alias_1
    ON _table_alias_0.a = _table_alias_1.a
)
SELECT
  _table_alias_2.b AS d
FROM _table_alias_2 AS _table_alias_2
LEFT JOIN _table_alias_0 AS _table_alias_3
  ON _table_alias_2.a = _table_alias_3.a
