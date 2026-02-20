WITH a AS (
  SELECT
    SEQ4() AS x
  FROM TABLE(GENERATOR(ROWCOUNT => 10))
), a_2 AS (
  SELECT
    SEQ4() AS x
  FROM TABLE(GENERATOR(ROWCOUNT => 10))
), b AS (
  SELECT
    SEQ4() * 2 AS y
  FROM TABLE(GENERATOR(ROWCOUNT => 501))
), _s3 AS (
  SELECT
    a_2.x,
    COUNT_IF(ENDSWITH(CAST(b.y AS TEXT), CAST(a_2.x AS TEXT))) AS sum_expr,
    COUNT_IF(STARTSWITH(CAST(b.y AS TEXT), CAST(a_2.x AS TEXT))) AS sum_expr_5
  FROM a_2 AS a_2
  JOIN b AS b
    ON ENDSWITH(CAST(b.y AS TEXT), CAST(a_2.x AS TEXT))
    OR STARTSWITH(CAST(b.y AS TEXT), CAST(a_2.x AS TEXT))
  GROUP BY
    1
)
SELECT
  a.x,
  COALESCE(_s3.sum_expr_5, 0) AS n_prefix,
  COALESCE(_s3.sum_expr, 0) AS n_suffix
FROM a AS a
LEFT JOIN _s3 AS _s3
  ON _s3.x = a.x
ORDER BY
  1 NULLS FIRST
