WITH _s1 AS (
  SELECT
    n_name,
    n_nationkey
  FROM tpch.nation
), _t1 AS (
  SELECT
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS agg_0,
    _s7.n_name AS cust_nation,
    EXTRACT(YEAR FROM lineitem.l_shipdate) AS l_year,
    _s1.n_name AS supp_nation
  FROM tpch.lineitem AS lineitem
  JOIN tpch.supplier AS supplier
    ON lineitem.l_suppkey = supplier.s_suppkey
  JOIN _s1 AS _s1
    ON _s1.n_nationkey = supplier.s_nationkey
  JOIN tpch.orders AS orders
    ON lineitem.l_orderkey = orders.o_orderkey
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey
  JOIN _s1 AS _s7
    ON _s7.n_nationkey = customer.c_nationkey
  WHERE
    EXTRACT(YEAR FROM lineitem.l_shipdate) IN (1995, 1996)
    AND (
      _s1.n_name = 'FRANCE' OR _s1.n_name = 'GERMANY'
    )
    AND (
      _s1.n_name = 'FRANCE' OR _s7.n_name = 'FRANCE'
    )
    AND (
      _s1.n_name = 'GERMANY' OR _s7.n_name = 'GERMANY'
    )
    AND (
      _s7.n_name = 'FRANCE' OR _s7.n_name = 'GERMANY'
    )
  GROUP BY
    _s7.n_name,
    EXTRACT(YEAR FROM lineitem.l_shipdate),
    _s1.n_name
)
SELECT
  supp_nation AS SUPP_NATION,
  cust_nation AS CUST_NATION,
  l_year AS L_YEAR,
  COALESCE(agg_0, 0) AS REVENUE
FROM _t1
ORDER BY
  supp_nation,
  cust_nation,
  l_year
