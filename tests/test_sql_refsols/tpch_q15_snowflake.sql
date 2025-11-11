WITH _t3 AS (
  SELECT
    l_discount,
    l_extendedprice,
    l_shipdate,
    l_suppkey
  FROM tpch.lineitem
  WHERE
    l_shipdate < CAST('1996-04-01' AS DATE)
    AND l_shipdate >= CAST('1996-01-01' AS DATE)
), _t1 AS (
  SELECT
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS sum_expr
  FROM _t3
  GROUP BY
    l_suppkey
), _s0 AS (
  SELECT
    MAX(COALESCE(sum_expr, 0)) AS max_total_revenue
  FROM _t1
), _s3 AS (
  SELECT
    l_suppkey,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS sum_expr
  FROM _t3
  GROUP BY
    1
)
SELECT
  supplier.s_suppkey AS S_SUPPKEY,
  supplier.s_name AS S_NAME,
  supplier.s_address AS S_ADDRESS,
  supplier.s_phone AS S_PHONE,
  COALESCE(_s3.sum_expr, 0) AS TOTAL_REVENUE
FROM _s0 AS _s0
CROSS JOIN tpch.supplier AS supplier
JOIN _s3 AS _s3
  ON _s0.max_total_revenue = COALESCE(_s3.sum_expr, 0)
  AND _s3.l_suppkey = supplier.s_suppkey
ORDER BY
  1 NULLS FIRST
