WITH _t1 AS (
  SELECT
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS agg_0,
    nation_2.n_name AS cust_nation,
    CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) AS l_year
  FROM tpch.lineitem AS lineitem
  JOIN tpch.supplier AS supplier
    ON lineitem.l_suppkey = supplier.s_suppkey
  JOIN tpch.nation AS nation
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN tpch.orders AS orders
    ON lineitem.l_orderkey = orders.o_orderkey
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey
  JOIN tpch.nation AS nation_2
    ON customer.c_nationkey = nation_2.n_nationkey
  WHERE
    CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) IN (1995, 1996)
    AND nation_2.n_name = 'FRANCE'
    AND nation_2.n_name = 'GERMANY'
  GROUP BY
    nation_2.n_name,
    CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER)
)
SELECT
  cust_nation AS SUPP_NATION,
  cust_nation AS CUST_NATION,
  l_year AS L_YEAR,
  COALESCE(agg_0, 0) AS REVENUE
FROM _t1
ORDER BY
  cust_nation,
  cust_nation,
  l_year
