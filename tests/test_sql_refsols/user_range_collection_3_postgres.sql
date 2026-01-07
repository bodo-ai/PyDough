WITH _s0 AS (
  SELECT
    x
  FROM GENERATE_SERIES(0, 9, 1) AS a(x)
), _s1 AS (
  SELECT
    y
  FROM GENERATE_SERIES(0, 1000, 2) AS b(y)
), _s4 AS (
  SELECT
    _s0.x,
    COUNT(*) AS n_rows
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON CAST(_s1.y AS TEXT) LIKE CONCAT(CAST(_s0.x AS TEXT), '%')
  GROUP BY
    1
), _s5 AS (
  SELECT
    _s2.x,
    COUNT(*) AS n_rows
  FROM _s0 AS _s2
  JOIN _s1 AS _s3
    ON CAST(_s3.y AS TEXT) LIKE CONCAT('%', CAST(_s2.x AS TEXT))
  GROUP BY
    1
)
SELECT
  _s4.x,
  _s4.n_rows AS n_prefix,
  _s5.n_rows AS n_suffix
FROM _s4 AS _s4
JOIN _s5 AS _s5
  ON _s4.x = _s5.x
ORDER BY
  1 NULLS FIRST
