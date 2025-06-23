WITH _s1 AS (
  SELECT
    n_name,
    n_nationkey
  FROM tpch.nation
  WHERE
    n_name = 'FRANCE' OR n_name = 'GERMANY'
), _s9 AS (
  SELECT
    _s7.n_name,
    orders.o_orderkey
  FROM tpch.orders AS orders
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey
  JOIN _s1 AS _s7
    ON _s7.n_nationkey = customer.c_nationkey
), _t1 AS (
  SELECT
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS sum_volume,
    _s1.n_name AS supp_nation,
    _s9.n_name AS cust_nation,
    EXTRACT(YEAR FROM CAST(lineitem.l_shipdate AS DATETIME)) AS l_year
  FROM tpch.lineitem AS lineitem
  JOIN tpch.supplier AS supplier
    ON lineitem.l_suppkey = supplier.s_suppkey
  JOIN _s1 AS _s1
    ON _s1.n_nationkey = supplier.s_nationkey
  JOIN _s9 AS _s9
    ON (
      _s1.n_name = 'FRANCE' OR _s9.n_name = 'FRANCE'
    )
    AND (
      _s1.n_name = 'GERMANY' OR _s9.n_name = 'GERMANY'
    )
    AND _s9.o_orderkey = lineitem.l_orderkey
  WHERE
    EXTRACT(YEAR FROM CAST(lineitem.l_shipdate AS DATETIME)) IN (1995, 1996)
  GROUP BY
    _s1.n_name,
    _s9.n_name,
    EXTRACT(YEAR FROM CAST(lineitem.l_shipdate AS DATETIME))
)
SELECT
  supp_nation AS SUPP_NATION,
  cust_nation AS CUST_NATION,
  l_year AS L_YEAR,
  COALESCE(sum_volume, 0) AS REVENUE
FROM _t1
ORDER BY
  supp_nation,
  cust_nation,
  l_year
