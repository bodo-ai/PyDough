WITH _t5 AS (
  SELECT
    l_discount,
    l_extendedprice,
    l_shipdate,
    l_suppkey
  FROM tpch.lineitem
  WHERE
    l_shipdate < CAST('1996-04-01' AS DATE)
    AND l_shipdate >= CAST('1996-01-01' AS DATE)
), _s1 AS (
  SELECT
    COALESCE(SUM(l_extendedprice * (
      1 - l_discount
    )), 0) AS total_revenue,
    l_suppkey
  FROM _t5
  GROUP BY
    l_suppkey
), _s2 AS (
  SELECT
    MAX(_s1.total_revenue) AS max_revenue
  FROM tpch.supplier AS supplier
  JOIN _s1 AS _s1
    ON _s1.l_suppkey = supplier.s_suppkey
), _s5 AS (
  SELECT
    COALESCE(SUM(l_extendedprice * (
      1 - l_discount
    )), 0) AS total_revenue,
    l_suppkey,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS sum_expr_3
  FROM _t5
  GROUP BY
    l_suppkey
)
SELECT
  supplier.s_suppkey AS S_SUPPKEY,
  supplier.s_name AS S_NAME,
  supplier.s_address AS S_ADDRESS,
  supplier.s_phone AS S_PHONE,
  _s5.total_revenue AS TOTAL_REVENUE
FROM _s2 AS _s2
CROSS JOIN tpch.supplier AS supplier
JOIN _s5 AS _s5
  ON _s2.max_revenue = COALESCE(_s5.sum_expr_3, 0)
  AND _s5.l_suppkey = supplier.s_suppkey
ORDER BY
  s_suppkey
