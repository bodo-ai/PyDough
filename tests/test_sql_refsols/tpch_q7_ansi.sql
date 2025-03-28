WITH _table_alias_1 AS (
  SELECT
    nation.n_nationkey AS key,
    nation.n_name AS name
  FROM tpch.nation AS nation
), _t1 AS (
  SELECT
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS agg_0,
    _table_alias_7.name AS cust_nation,
    EXTRACT(YEAR FROM lineitem.l_shipdate) AS l_year,
    _table_alias_1.name AS supp_nation
  FROM tpch.lineitem AS lineitem
  LEFT JOIN tpch.supplier AS supplier
    ON lineitem.l_suppkey = supplier.s_suppkey
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_1.key = supplier.s_nationkey
  LEFT JOIN tpch.orders AS orders
    ON lineitem.l_orderkey = orders.o_orderkey
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey
  JOIN _table_alias_1 AS _table_alias_7
    ON _table_alias_7.key = customer.c_nationkey
  WHERE
    (
      _table_alias_1.name = 'FRANCE' OR _table_alias_1.name = 'GERMANY'
    )
    AND (
      _table_alias_1.name = 'FRANCE' OR _table_alias_7.name = 'FRANCE'
    )
    AND (
      _table_alias_1.name = 'GERMANY' OR _table_alias_7.name = 'GERMANY'
    )
    AND (
      _table_alias_7.name = 'FRANCE' OR _table_alias_7.name = 'GERMANY'
    )
    AND lineitem.l_shipdate <= CAST('1996-12-31' AS DATE)
    AND lineitem.l_shipdate >= CAST('1995-01-01' AS DATE)
  GROUP BY
    _table_alias_7.name,
    EXTRACT(YEAR FROM lineitem.l_shipdate),
    _table_alias_1.name
)
SELECT
  _t1.supp_nation AS SUPP_NATION,
  _t1.cust_nation AS CUST_NATION,
  _t1.l_year AS L_YEAR,
  COALESCE(_t1.agg_0, 0) AS REVENUE
FROM _t1 AS _t1
ORDER BY
  _t1.supp_nation,
  _t1.cust_nation,
  _t1.l_year
