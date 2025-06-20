WITH _t1 AS (
  SELECT
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS agg_0,
    l_suppkey AS supplier_key
  FROM tpch.lineitem
  WHERE
    l_shipdate < CAST('1996-04-01' AS DATE)
    AND l_shipdate >= CAST('1996-01-01' AS DATE)
  GROUP BY
    l_suppkey
), _s5 AS (
  SELECT
    MAX(COALESCE(_t1.agg_0, 0)) AS max_revenue
  FROM tpch.supplier AS _s0
  JOIN _t1 AS _t1
    ON _s0.s_suppkey = _t1.supplier_key
), _t5 AS (
  SELECT
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS agg_1,
    l_suppkey AS supplier_key
  FROM tpch.lineitem
  WHERE
    l_shipdate < CAST('1996-04-01' AS DATE)
    AND l_shipdate >= CAST('1996-01-01' AS DATE)
  GROUP BY
    l_suppkey
)
SELECT
  _s4.s_suppkey AS S_SUPPKEY,
  _s4.s_name AS S_NAME,
  _s4.s_address AS S_ADDRESS,
  _s4.s_phone AS S_PHONE,
  COALESCE(_t5.agg_1, 0) AS TOTAL_REVENUE
FROM _s5 AS _s5
CROSS JOIN tpch.supplier AS _s4
JOIN _t5 AS _t5
  ON _s4.s_suppkey = _t5.supplier_key AND _s5.max_revenue = COALESCE(_t5.agg_1, 0)
ORDER BY
  s_suppkey
