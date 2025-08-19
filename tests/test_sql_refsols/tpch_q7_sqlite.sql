WITH _s9 AS (
  SELECT
    nation.n_name,
    orders.o_orderkey
  FROM tpch.orders AS orders
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey
    AND (
      nation.n_name = 'FRANCE' OR nation.n_name = 'GERMANY'
    )
)
SELECT
  nation.n_name AS SUPP_NATION,
  _s9.n_name AS CUST_NATION,
  CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) AS L_YEAR,
  COALESCE(SUM(lineitem.l_extendedprice * (
    1 - lineitem.l_discount
  )), 0) AS REVENUE
FROM tpch.lineitem AS lineitem
JOIN tpch.supplier AS supplier
  ON lineitem.l_suppkey = supplier.s_suppkey
JOIN tpch.nation AS nation
  ON nation.n_nationkey = supplier.s_nationkey
JOIN _s9 AS _s9
  ON (
    _s9.n_name = 'FRANCE' OR nation.n_name = 'FRANCE'
  )
  AND (
    _s9.n_name = 'GERMANY' OR nation.n_name = 'GERMANY'
  )
  AND _s9.o_orderkey = lineitem.l_orderkey
  AND (
    nation.n_name = 'FRANCE' OR nation.n_name = 'GERMANY'
  )
WHERE
  CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) IN (1995, 1996)
GROUP BY
  CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER),
  _s9.n_name,
  nation.n_name
ORDER BY
  supp_nation,
  _s9.n_name,
  l_year
