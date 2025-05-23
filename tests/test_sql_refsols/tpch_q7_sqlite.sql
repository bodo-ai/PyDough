WITH _s1 AS (
  SELECT
    n_nationkey AS key,
    n_name AS name
  FROM tpch.nation
  WHERE
    n_name = 'FRANCE' OR n_name = 'GERMANY'
), _s9 AS (
  SELECT
    orders.o_orderkey AS key,
    _s7.name AS name_8
  FROM tpch.orders AS orders
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey
  JOIN _s1 AS _s7
    ON _s7.key = customer.c_nationkey
), _t1 AS (
  SELECT
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS agg_0,
    _s9.name_8 AS cust_nation,
    CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) AS l_year,
    _s1.name AS supp_nation
  FROM tpch.lineitem AS lineitem
  JOIN tpch.supplier AS supplier
    ON lineitem.l_suppkey = supplier.s_suppkey
  JOIN _s1 AS _s1
    ON _s1.key = supplier.s_nationkey
  JOIN _s9 AS _s9
    ON (
      _s1.name = 'FRANCE' OR _s9.name_8 = 'FRANCE'
    )
    AND (
      _s1.name = 'GERMANY' OR _s9.name_8 = 'GERMANY'
    )
    AND _s9.key = lineitem.l_orderkey
  WHERE
    lineitem.l_shipdate <= '1996-12-31' AND lineitem.l_shipdate >= '1995-01-01'
  GROUP BY
    _s9.name_8,
    CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER),
    _s1.name
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
