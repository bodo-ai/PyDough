WITH _s0 AS (
  SELECT
    a
  FROM table
), _u_0 AS (
  SELECT
    a AS _u_1
  FROM _s0
  GROUP BY
    1
)
SELECT
  _s0.a
FROM _s0 AS _s0
LEFT JOIN _u_0 AS _u_0
  ON _s0.a = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
