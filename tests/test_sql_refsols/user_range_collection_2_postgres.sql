WITH _s3 AS (
  SELECT
    a_2.x,
    SUM(
      CASE
        WHEN CAST(b.y AS TEXT) LIKE (
          CONCAT('%', CAST(a_2.x AS TEXT))
        )
        THEN 1
        ELSE 0
      END
    ) AS sum_expr,
    SUM(
      CASE
        WHEN CAST(b.y AS TEXT) LIKE (
          CONCAT(CAST(a_2.x AS TEXT), '%')
        )
        THEN 1
        ELSE 0
      END
    ) AS sum_expr_5
  FROM GENERATE_SERIES(0, 9, 1) AS a_2(x)
  JOIN GENERATE_SERIES(0, 1000, 2) AS b(y)
    ON CAST(b.y AS TEXT) LIKE CONCAT('%', CAST(a_2.x AS TEXT))
    OR CAST(b.y AS TEXT) LIKE CONCAT(CAST(a_2.x AS TEXT), '%')
  GROUP BY
    1
)
SELECT
  a.x,
  COALESCE(_s3.sum_expr_5, 0) AS n_prefix,
  COALESCE(_s3.sum_expr, 0) AS n_suffix
FROM GENERATE_SERIES(0, 9, 1) AS a(x)
LEFT JOIN _s3 AS _s3
  ON _s3.x = a.x
ORDER BY
  1 NULLS FIRST
