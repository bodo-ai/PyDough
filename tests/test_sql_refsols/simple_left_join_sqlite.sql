WITH _t0 AS (
  SELECT
    a
  FROM table
)
SELECT
  _t0.a
FROM _t0 AS _t0
LEFT JOIN _t0 AS _t1
  ON _t0.a = _t1.a
