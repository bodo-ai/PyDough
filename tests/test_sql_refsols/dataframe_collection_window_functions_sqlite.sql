WITH _t AS (
  SELECT
    customer.c_acctbal,
    customer.c_custkey,
    customer.c_mktsegment,
    customer.c_name,
    customers_filters.column2 AS mrk_segment,
    nation.n_name,
    customers_filters.column1 AS nation_name,
    NTILE(1000) OVER (ORDER BY customer.c_acctbal) AS _w
  FROM (VALUES
    ('UNITED STATES', 'BUILDING'),
    ('JAPAN', 'AUTOMOBILE'),
    ('BRAZIL', 'MACHINERY')) AS customers_filters
  CROSS JOIN tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey
), _s6 AS (
  SELECT
    _t.c_custkey,
    MAX(_t.c_acctbal) AS anything_c_acctbal,
    MAX(_t.c_name) AS anything_c_name,
    MAX(_t.mrk_segment) AS anything_mrk_segment,
    MAX(_t.nation_name) AS anything_nation_name,
    COUNT(orders.o_custkey) AS count_o_custkey
  FROM _t AS _t
  LEFT JOIN tpch.orders AS orders
    ON _t.c_custkey = orders.o_custkey
  WHERE
    _t._w > 996 AND _t.c_mktsegment = _t.mrk_segment AND _t.n_name = _t.nation_name
  GROUP BY
    1
), _t4 AS (
  SELECT
    o_custkey,
    (
      CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) - CAST(STRFTIME('%Y', LAG(o_orderdate, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderdate)) AS INTEGER)
    ) * 12 + CAST(STRFTIME('%m', o_orderdate) AS INTEGER) - CAST(STRFTIME('%m', LAG(o_orderdate, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderdate)) AS INTEGER) AS month_diff
  FROM tpch.orders
), _s7 AS (
  SELECT
    o_custkey,
    AVG(month_diff) AS avg_month_diff
  FROM _t4
  GROUP BY
    1
), _t6 AS (
  SELECT
    o_custkey,
    o_totalprice - LEAD(o_totalprice, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS price_diff
  FROM tpch.orders
), _s9 AS (
  SELECT
    o_custkey,
    AVG(price_diff) AS avg_price_diff
  FROM _t6
  GROUP BY
    1
)
SELECT
  _s6.anything_c_name AS name,
  ROW_NUMBER() OVER (PARTITION BY _s6.anything_mrk_segment, _s6.anything_nation_name ORDER BY _s6.anything_c_acctbal DESC) AS ranking_balance,
  COALESCE(_s6.count_o_custkey, 0) AS n_orders,
  _s7.avg_month_diff AS avg_month_orders,
  _s9.avg_price_diff,
  CAST(_s6.anything_c_acctbal AS REAL) / SUM(_s6.anything_c_acctbal) OVER () AS proportion,
  IIF(_s6.anything_c_acctbal > AVG(_s6.anything_c_acctbal) OVER (), TRUE, FALSE) AS above_avg,
  COUNT(_s6.anything_c_acctbal) OVER (ORDER BY _s6.anything_c_acctbal ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_poorer,
  CAST(_s6.anything_c_acctbal AS REAL) / COUNT(*) OVER () AS ratio
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.c_custkey = _s7.o_custkey
LEFT JOIN _s9 AS _s9
  ON _s6.c_custkey = _s9.o_custkey
ORDER BY
  1
