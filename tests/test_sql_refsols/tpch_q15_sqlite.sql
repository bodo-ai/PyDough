WITH _t5 AS (
  SELECT
    l_discount AS discount,
    l_extendedprice AS extended_price,
    l_shipdate AS ship_date,
    l_suppkey AS supplier_key
  FROM tpch.lineitem
  WHERE
    l_shipdate < '1996-04-01' AND l_shipdate >= '1996-01-01'
), _t2 AS (
  SELECT
    SUM(extended_price * (
      1 - discount
    )) AS agg_0,
    supplier_key
  FROM _t5
  GROUP BY
    supplier_key
), _t2_2 AS (
  SELECT
    MAX(COALESCE(_t2.agg_0, 0)) AS max_revenue
  FROM tpch.supplier AS supplier
  JOIN _t2 AS _t2
    ON _t2.supplier_key = supplier.s_suppkey
), _t6 AS (
  SELECT
    SUM(extended_price * (
      1 - discount
    )) AS agg_1,
    supplier_key
  FROM _t5
  GROUP BY
    supplier_key
)
SELECT
  supplier.s_suppkey AS S_SUPPKEY,
  supplier.s_name AS S_NAME,
  supplier.s_address AS S_ADDRESS,
  supplier.s_phone AS S_PHONE,
  COALESCE(_t6.agg_1, 0) AS TOTAL_REVENUE
FROM _t2_2 AS _t2
CROSS JOIN tpch.supplier AS supplier
JOIN _t6 AS _t6
  ON _t2.max_revenue = COALESCE(_t6.agg_1, 0)
  AND _t6.supplier_key = supplier.s_suppkey
ORDER BY
  s_suppkey
