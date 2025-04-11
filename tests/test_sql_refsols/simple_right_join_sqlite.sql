WITH _s0 AS (
  SELECT
    a
  FROM table
)
SELECT
  _s0.a
FROM _s0 AS _s0
RIGHT JOIN _s0 AS _s1
  ON _s0.a = _s1.a
