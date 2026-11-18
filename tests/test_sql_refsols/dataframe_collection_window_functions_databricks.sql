WITH _t1 AS (
  SELECT
    customer.c_acctbal,
    customer.c_custkey,
    customer.c_name,
    customers_filters.nation_name
  FROM VALUES
    ('UNITED STATES', 'BUILDING'),
    ('JAPAN', 'AUTOMOBILE'),
    ('BRAZIL', 'MACHINERY') AS customers_filters(nation_name, mrk_segment)
  CROSS JOIN tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey
  QUALIFY
    NTILE(1000) OVER (ORDER BY customer.c_acctbal NULLS LAST) > 996
    AND customer.c_mktsegment = customers_filters.mrk_segment
    AND customers_filters.nation_name = nation.n_name
), _s5 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    1
), _t4 AS (
  SELECT
    o_custkey,
    (
      YEAR(o_orderdate) - YEAR(
        LAG(o_orderdate, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderdate NULLS LAST)
      )
    ) * 12 + MONTH(o_orderdate) - MONTH(
      LAG(o_orderdate, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderdate NULLS LAST)
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
    o_totalprice - LEAD(o_totalprice, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderdate NULLS LAST) AS price_diff
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
  _t1.c_name AS name,
  ROW_NUMBER() OVER (PARTITION BY _t1.nation_name ORDER BY _t1.c_acctbal DESC NULLS FIRST) AS ranking_balance,
  COALESCE(_s5.n_rows, 0) AS n_orders,
  _s7.avg_month_diff AS avg_month_orders,
  _s9.avg_price_diff,
  _t1.c_acctbal / SUM(_t1.c_acctbal) OVER () AS proportion,
  IF(_t1.c_acctbal > AVG(CAST(_t1.c_acctbal AS DOUBLE)) OVER (), TRUE, FALSE) AS above_avg,
  COUNT(_t1.c_acctbal) OVER (
    ORDER BY _t1.c_acctbal NULLS LAST
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS n_poorer,
  _t1.c_acctbal / COUNT(*) OVER () AS ratio
FROM _t1 AS _t1
LEFT JOIN _s5 AS _s5
  ON _s5.o_custkey = _t1.c_custkey
LEFT JOIN _s7 AS _s7
  ON _s7.o_custkey = _t1.c_custkey
LEFT JOIN _s9 AS _s9
  ON _s9.o_custkey = _t1.c_custkey
ORDER BY
  1
