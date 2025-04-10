WITH _t6 AS (
  SELECT
    l_discount AS discount,
    l_extendedprice AS extended_price,
    l_shipdate AS ship_date,
    l_suppkey AS supplier_key
  FROM tpch.lineitem
  WHERE
    l_shipdate < CAST('1996-04-01' AS DATE)
    AND l_shipdate >= CAST('1996-01-01' AS DATE)
), _t3 AS (
  SELECT
    SUM(extended_price * (
      1 - discount
    )) AS agg_0,
    supplier_key
  FROM _t6
  GROUP BY
    supplier_key
), _s1 AS (
  SELECT
    MAX(COALESCE(agg_0, 0)) AS agg_0,
    supplier_key
  FROM _t3
  GROUP BY
    supplier_key
), _s2 AS (
  SELECT
    MAX(_s1.agg_0) AS max_revenue
  FROM tpch.supplier AS supplier
  JOIN _s1 AS _s1
    ON _s1.supplier_key = supplier.s_suppkey
), _t7 AS (
  SELECT
    SUM(extended_price * (
      1 - discount
    )) AS agg_1,
    supplier_key
  FROM _t6
  GROUP BY
    supplier_key
)
SELECT
  supplier.s_suppkey AS S_SUPPKEY,
  supplier.s_name AS S_NAME,
  supplier.s_address AS S_ADDRESS,
  supplier.s_phone AS S_PHONE,
  COALESCE(_t7.agg_1, 0) AS TOTAL_REVENUE
FROM _s2 AS _s2
CROSS JOIN tpch.supplier AS supplier
JOIN _t7 AS _t7
  ON _s2.max_revenue = COALESCE(_t7.agg_1, 0)
  AND _t7.supplier_key = supplier.s_suppkey
ORDER BY
  s_suppkey
