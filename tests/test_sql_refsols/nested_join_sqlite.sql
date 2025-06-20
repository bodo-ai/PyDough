WITH _s6 AS (
  SELECT
    a
  FROM table
)
SELECT
  _s1.b AS d
FROM table AS _s0
JOIN table AS _s1
  ON _s0.a = _s1.a
LEFT JOIN _s6 AS _s6
  ON _s0.a = _s6.a
