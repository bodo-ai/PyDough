WITH _t4 AS (
  SELECT
    l_discount AS discount,
    l_extendedprice AS extended_price,
    l_shipdate AS ship_date,
    l_suppkey AS supplier_key
  FROM tpch.lineitem
), _t1 AS (
  SELECT
    SUM(extended_price * (
      1 - discount
    )) AS agg_0,
    supplier_key
  FROM _t4
  WHERE
    ship_date < '1996-04-01' AND ship_date >= '1996-01-01'
  GROUP BY
    supplier_key
), _s2 AS (
  SELECT
    MAX(COALESCE(_t1.agg_0, 0)) AS max_revenue
  FROM tpch.supplier AS supplier
  JOIN _t1 AS _t1
    ON _t1.supplier_key = supplier.s_suppkey
), _t5 AS (
  SELECT
    SUM(extended_price * (
      1 - discount
    )) AS agg_1,
    supplier_key
  FROM _t4
  WHERE
    ship_date < '1996-04-01' AND ship_date >= '1996-01-01'
  GROUP BY
    supplier_key
)
SELECT
  supplier.s_suppkey AS S_SUPPKEY,
  supplier.s_name AS S_NAME,
  supplier.s_address AS S_ADDRESS,
  supplier.s_phone AS S_PHONE,
  COALESCE(_t5.agg_1, 0) AS TOTAL_REVENUE
FROM _s2 AS _s2
CROSS JOIN tpch.supplier AS supplier
JOIN _t5 AS _t5
  ON _s2.max_revenue = COALESCE(_t5.agg_1, 0)
  AND _t5.supplier_key = supplier.s_suppkey
ORDER BY
  s_suppkey
