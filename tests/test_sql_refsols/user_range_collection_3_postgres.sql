WITH _s5 AS (
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
  a.x,
  COUNT(*) AS n_prefix,
  MAX(_s5.n_rows) AS n_suffix
FROM GENERATE_SERIES(0, 9, 1) AS a(x)
JOIN GENERATE_SERIES(0, 1000, 2) AS b(y)
  ON CAST(b.y AS TEXT) LIKE CONCAT(CAST(a.x AS TEXT), '%')
JOIN _s5 AS _s5
  ON _s5.x = a.x
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
