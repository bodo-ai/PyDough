WITH _s0 AS (
  SELECT
    table.a AS a
  FROM table AS table
), _u_0 AS (
  SELECT
    _s1.a AS _u_1
  FROM _s0 AS _s1
  GROUP BY
    _s1.a
)
SELECT
  _s0.a AS a
FROM _s0 AS _s0
LEFT JOIN _u_0 AS _u_0
  ON _s0.a = _u_0._u_1
WHERE
  _u_0._u_1 IS NULL
