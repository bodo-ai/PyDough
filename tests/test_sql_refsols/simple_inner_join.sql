SELECT
  _table_alias_0.a AS a,
  _table_alias_1.b AS b
FROM (
  SELECT
    a,
    b
  FROM table
) AS _table_alias_0
INNER JOIN (
  SELECT
    a,
    b
  FROM table
) AS _table_alias_1
  ON _table_alias_0.a = _table_alias_1.a
