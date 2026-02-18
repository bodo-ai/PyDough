WITH _s4 AS (
  SELECT
    a.x,
    COUNT(*) AS n_rows
  FROM GENERATE_SERIES(0, 9, 1) AS a(x)
  JOIN GENERATE_SERIES(0, 1000, 2) AS b(y)
    ON CAST(b.y AS TEXT) LIKE CONCAT(CAST(a.x AS TEXT), '%')
  GROUP BY
    1
), _s5 AS (
  SELECT
    a_2.x,
    COUNT(*) AS n_rows
  FROM GENERATE_SERIES(0, 9, 1) AS a_2(x)
  JOIN GENERATE_SERIES(0, 1000, 2) AS b_2(y)
    ON CAST(b_2.y AS TEXT) LIKE CONCAT('%', CAST(a_2.x AS TEXT))
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
