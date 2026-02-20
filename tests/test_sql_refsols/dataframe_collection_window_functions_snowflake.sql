WITH _t2 AS (
  SELECT
    customer.c_acctbal,
    customer.c_custkey,
    customer.c_name,
    customers_filters.nation_name
  FROM (VALUES
    ('UNITED STATES', 'BUILDING'),
    ('JAPAN', 'AUTOMOBILE'),
    ('BRAZIL', 'MACHINERY')) AS customers_filters(nation_name, mrk_segment)
  CROSS JOIN tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey
  QUALIFY
    NTILE(1000) OVER (ORDER BY customer.c_acctbal) > 996
    AND customer.c_mktsegment = customers_filters.mrk_segment
    AND customers_filters.nation_name = nation.n_name
), _s6 AS (
  SELECT
    _t2.c_custkey,
    ANY_VALUE(_t2.c_acctbal) AS anything_c_acctbal,
    ANY_VALUE(_t2.c_name) AS anything_c_name,
    ANY_VALUE(_t2.nation_name) AS anything_nation_name,
    COUNT(orders.o_custkey) AS count_o_custkey
  FROM _t2 AS _t2
  LEFT JOIN tpch.orders AS orders
    ON _t2.c_custkey = orders.o_custkey
  GROUP BY
    1
), _t4 AS (
  SELECT
    o_custkey,
    DATEDIFF(
      MONTH,
      CAST(LAG(o_orderdate, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS DATETIME),
      CAST(o_orderdate AS DATETIME)
    ) AS month_diff
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
  ROW_NUMBER() OVER (PARTITION BY _s6.anything_nation_name ORDER BY _s6.anything_c_acctbal DESC) AS ranking_balance,
  COALESCE(_s6.count_o_custkey, 0) AS n_orders,
  _s7.avg_month_diff AS avg_month_orders,
  _s9.avg_price_diff,
  _s6.anything_c_acctbal / SUM(_s6.anything_c_acctbal) OVER () AS proportion,
  IFF(
    _s6.anything_c_acctbal > AVG(CAST(_s6.anything_c_acctbal AS DOUBLE)) OVER (),
    TRUE,
    FALSE
  ) AS above_avg,
  COUNT(_s6.anything_c_acctbal) OVER (ORDER BY _s6.anything_c_acctbal ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_poorer,
  _s6.anything_c_acctbal / COUNT(*) OVER () AS ratio
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.c_custkey = _s7.o_custkey
LEFT JOIN _s9 AS _s9
  ON _s6.c_custkey = _s9.o_custkey
ORDER BY
  1 NULLS FIRST
