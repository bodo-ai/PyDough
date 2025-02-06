SELECT
  _table_alias_0.a AS a
FROM (
  SELECT
    a,
    b
  FROM table
) AS _table_alias_0
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM (
      SELECT
        a,
        b
      FROM table
    ) AS _table_alias_1
    WHERE
      _table_alias_0.a = _table_alias_1.a
  )
