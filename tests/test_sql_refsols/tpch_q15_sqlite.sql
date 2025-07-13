WITH _t5 AS (
  SELECT
    l_discount,
    l_extendedprice,
    l_shipdate,
    l_suppkey
  FROM tpch.lineitem
  WHERE
    l_shipdate < '1996-04-01' AND l_shipdate >= '1996-01-01'
), _s1 AS (
  SELECT
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS sum_expr_2,
    l_suppkey
  FROM _t5
  GROUP BY
    l_suppkey
), _s2 AS (
  SELECT
    MAX(COALESCE(_s1.sum_expr_2, 0)) AS max_revenue
  FROM tpch.supplier AS supplier
  JOIN _s1 AS _s1
    ON _s1.l_suppkey = supplier.s_suppkey
), _s5 AS (
  SELECT
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS sum_expr_3,
    l_suppkey
  FROM _t5
  GROUP BY
    l_suppkey
)
SELECT
  supplier.s_suppkey AS S_SUPPKEY,
  supplier.s_name AS S_NAME,
  supplier.s_address AS S_ADDRESS,
  supplier.s_phone AS S_PHONE,
  COALESCE(_s5.sum_expr_3, 0) AS TOTAL_REVENUE
FROM _s2 AS _s2
CROSS JOIN tpch.supplier AS supplier
JOIN _s5 AS _s5
  ON _s2.max_revenue = COALESCE(_s5.sum_expr_3, 0)
  AND _s5.l_suppkey = supplier.s_suppkey
ORDER BY
  s_suppkey
