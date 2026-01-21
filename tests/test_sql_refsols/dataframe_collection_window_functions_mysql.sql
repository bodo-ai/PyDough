WITH _t AS (
  SELECT
    CUSTOMER.c_acctbal,
    CUSTOMER.c_custkey,
    CUSTOMER.c_mktsegment,
    CUSTOMER.c_name,
    customers_filters.mrk_segment,
    NATION.n_name,
    customers_filters.nation_name,
    NTILE(1000) OVER (ORDER BY CASE WHEN CUSTOMER.c_acctbal IS NULL THEN 1 ELSE 0 END, CUSTOMER.c_acctbal) AS _w
  FROM (VALUES
    ROW('UNITED STATES', 'BUILDING'),
    ROW('JAPAN', 'AUTOMOBILE'),
    ROW('BRAZIL', 'MACHINERY')) AS customers_filters(nation_name, mrk_segment)
  CROSS JOIN tpch.CUSTOMER AS CUSTOMER
  JOIN tpch.NATION AS NATION
    ON CUSTOMER.c_nationkey = NATION.n_nationkey
), _s6 AS (
  SELECT
    _t.c_custkey,
    ANY_VALUE(_t.c_acctbal) AS anything_c_acctbal,
    ANY_VALUE(_t.c_name) AS anything_c_name,
    ANY_VALUE(_t.mrk_segment) AS anything_mrk_segment,
    ANY_VALUE(_t.nation_name) AS anything_nation_name,
    COUNT(ORDERS.o_custkey) AS count_o_custkey
  FROM _t AS _t
  LEFT JOIN tpch.ORDERS AS ORDERS
    ON ORDERS.o_custkey = _t.c_custkey
  WHERE
    _t._w > 996 AND _t.c_mktsegment = _t.mrk_segment AND _t.n_name = _t.nation_name
  GROUP BY
    1
), _t4 AS (
  SELECT
    o_custkey,
    (
      YEAR(o_orderdate) - YEAR(
        LAG(o_orderdate, 1) OVER (PARTITION BY o_custkey ORDER BY CASE WHEN o_orderdate IS NULL THEN 1 ELSE 0 END, o_orderdate)
      )
    ) * 12 + (
      MONTH(o_orderdate) - MONTH(
        LAG(o_orderdate, 1) OVER (PARTITION BY o_custkey ORDER BY CASE WHEN o_orderdate IS NULL THEN 1 ELSE 0 END, o_orderdate)
      )
    ) AS month_diff
  FROM tpch.ORDERS
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
    o_totalprice - LEAD(o_totalprice, 1) OVER (PARTITION BY o_custkey ORDER BY CASE WHEN o_orderdate IS NULL THEN 1 ELSE 0 END, o_orderdate) AS price_diff
  FROM tpch.ORDERS
), _s9 AS (
  SELECT
    o_custkey,
    AVG(price_diff) AS avg_price_diff
  FROM _t6
  GROUP BY
    1
)
SELECT
  _s6.anything_c_name COLLATE utf8mb4_bin AS name,
  ROW_NUMBER() OVER (PARTITION BY _s6.anything_mrk_segment, _s6.anything_nation_name ORDER BY CASE WHEN _s6.anything_c_acctbal IS NULL THEN 1 ELSE 0 END DESC, _s6.anything_c_acctbal DESC) AS ranking_balance,
  COALESCE(_s6.count_o_custkey, 0) AS n_orders,
  _s7.avg_month_diff AS avg_month_orders,
  _s9.avg_price_diff,
  _s6.anything_c_acctbal / SUM(_s6.anything_c_acctbal) OVER () AS proportion,
  CASE
    WHEN _s6.anything_c_acctbal > AVG(_s6.anything_c_acctbal) OVER ()
    THEN TRUE
    ELSE FALSE
  END AS above_avg,
  COUNT(_s6.anything_c_acctbal) OVER (ORDER BY _s6.anything_c_acctbal ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_poorer,
  _s6.anything_c_acctbal / COUNT(*) OVER () AS ratio
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.c_custkey = _s7.o_custkey
LEFT JOIN _s9 AS _s9
  ON _s6.c_custkey = _s9.o_custkey
ORDER BY
  1
