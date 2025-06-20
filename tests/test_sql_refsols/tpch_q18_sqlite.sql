WITH _t0 AS (
  SELECT
    SUM(l_quantity) AS agg_0,
    l_orderkey AS order_key
  FROM tpch.lineitem
  GROUP BY
    l_orderkey
)
SELECT
  _s1.c_name AS C_NAME,
  _s1.c_custkey AS C_CUSTKEY,
  _s0.o_orderkey AS O_ORDERKEY,
  _s0.o_orderdate AS O_ORDERDATE,
  _s0.o_totalprice AS O_TOTALPRICE,
  COALESCE(_t0.agg_0, 0) AS TOTAL_QUANTITY
FROM tpch.orders AS _s0
JOIN tpch.customer AS _s1
  ON _s0.o_custkey = _s1.c_custkey
JOIN _t0 AS _t0
  ON _s0.o_orderkey = _t0.order_key
WHERE
  NOT _t0.agg_0 IS NULL AND _t0.agg_0 > 300
ORDER BY
  o_totalprice DESC,
  o_orderdate
LIMIT 10
