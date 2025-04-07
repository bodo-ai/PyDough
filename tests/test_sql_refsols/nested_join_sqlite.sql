WITH _t0 AS (
  SELECT
    a
  FROM table
)
SELECT
  table.b AS d
FROM _t0 AS _t0
JOIN table AS table
  ON _t0.a = table.a
LEFT JOIN _t0 AS _t3
  ON _t0.a = _t3.a
