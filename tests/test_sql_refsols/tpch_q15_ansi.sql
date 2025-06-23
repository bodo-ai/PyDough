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
), _t2 AS (
  SELECT
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS sum_expr_2,
    l_suppkey AS supplier_key
  FROM _t5
  GROUP BY
    l_suppkey
), _s2 AS (
  SELECT
    MAX(COALESCE(_t2.sum_expr_2, 0)) AS max_revenue
  FROM tpch.supplier AS supplier
  JOIN _t2 AS _t2
    ON _t2.supplier_key = supplier.s_suppkey
), _t6 AS (
  SELECT
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS sum_expr_3,
    l_suppkey AS supplier_key
  FROM _t5
  GROUP BY
    l_suppkey
)
SELECT
  supplier.s_suppkey AS S_SUPPKEY,
  supplier.s_name AS S_NAME,
  supplier.s_address AS S_ADDRESS,
  supplier.s_phone AS S_PHONE,
  COALESCE(_t6.sum_expr_3, 0) AS TOTAL_REVENUE
FROM _s2 AS _s2
CROSS JOIN tpch.supplier AS supplier
JOIN _t6 AS _t6
  ON _s2.max_revenue = COALESCE(_t6.sum_expr_3, 0)
  AND _t6.supplier_key = supplier.s_suppkey
ORDER BY
  s_suppkey
