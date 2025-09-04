WITH _t3 AS (
  SELECT
    l_discount,
    l_extendedprice,
    l_shipdate,
    l_suppkey
  FROM tpch.LINEITEM
  WHERE
    l_shipdate < CAST('1996-04-01' AS DATE)
    AND l_shipdate >= CAST('1996-01-01' AS DATE)
), _s1 AS (
  SELECT
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS agg_0,
    l_suppkey
  FROM _t3
  GROUP BY
    2
), _s2 AS (
  SELECT
    MAX(COALESCE(_s1.agg_0, 0)) AS max_revenue
  FROM tpch.SUPPLIER AS SUPPLIER
  JOIN _s1 AS _s1
    ON SUPPLIER.s_suppkey = _s1.l_suppkey
), _s5 AS (
  SELECT
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS agg_1,
    l_suppkey
  FROM _t3
  GROUP BY
    2
)
SELECT
  SUPPLIER.s_suppkey AS S_SUPPKEY,
  SUPPLIER.s_name AS S_NAME,
  SUPPLIER.s_address AS S_ADDRESS,
  SUPPLIER.s_phone AS S_PHONE,
  COALESCE(_s5.agg_1, 0) AS TOTAL_REVENUE
FROM _s2 AS _s2
CROSS JOIN tpch.SUPPLIER AS SUPPLIER
JOIN _s5 AS _s5
  ON SUPPLIER.s_suppkey = _s5.l_suppkey AND _s2.max_revenue = COALESCE(_s5.agg_1, 0)
ORDER BY
  1
