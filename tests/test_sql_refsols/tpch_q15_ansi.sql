WITH _t7 AS (
  SELECT
    l_discount AS discount,
    l_extendedprice AS extended_price,
    l_shipdate AS ship_date,
    l_suppkey AS supplier_key
  FROM tpch.lineitem
  WHERE
    l_shipdate < CAST('1996-04-01' AS DATE)
    AND l_shipdate >= CAST('1996-01-01' AS DATE)
), _t1 AS (
  SELECT
    SUM(extended_price * (
      1 - discount
    )) AS agg_0,
    supplier_key
  FROM _t7
  GROUP BY
    supplier_key
), _t2 AS (
  SELECT
    MAX(COALESCE(_t1.agg_0, 0)) AS max_revenue
  FROM tpch.supplier AS supplier
  LEFT JOIN _t1 AS _t1
    ON _t1.supplier_key = supplier.s_suppkey
), _t5_2 AS (
  SELECT
    SUM(extended_price * (
      1 - discount
    )) AS agg_1,
    supplier_key
  FROM _t7
  GROUP BY
    supplier_key
)
SELECT
  supplier.s_suppkey AS S_SUPPKEY,
  supplier.s_name AS S_NAME,
  supplier.s_address AS S_ADDRESS,
  supplier.s_phone AS S_PHONE,
  COALESCE(_t5.agg_1, 0) AS TOTAL_REVENUE
FROM _t2 AS _t2
CROSS JOIN tpch.supplier AS supplier
LEFT JOIN _t5_2 AS _t5
  ON _t5.supplier_key = supplier.s_suppkey
WHERE
  _t2.max_revenue = COALESCE(_t5.agg_1, 0)
ORDER BY
  supplier.s_suppkey
