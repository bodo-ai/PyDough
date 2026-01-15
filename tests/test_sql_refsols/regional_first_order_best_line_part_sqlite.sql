WITH _t AS (
  SELECT
    nation.n_regionkey,
    orders.o_orderkey,
    ROW_NUMBER() OVER (PARTITION BY nation.n_regionkey ORDER BY orders.o_orderdate, orders.o_orderkey) AS _w
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1992
    AND customer.c_custkey = orders.o_custkey
), _t_2 AS (
  SELECT
    lineitem.l_partkey,
    _t.n_regionkey,
    ROW_NUMBER() OVER (PARTITION BY _t.n_regionkey ORDER BY lineitem.l_quantity DESC, lineitem.l_linenumber) AS _w
  FROM _t AS _t
  JOIN tpch.lineitem AS lineitem
    ON CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) = 1992
    AND _t.o_orderkey = lineitem.l_orderkey
  WHERE
    _t._w = 1
), _s9 AS (
  SELECT
    _t.n_regionkey,
    part.p_name
  FROM _t_2 AS _t
  JOIN tpch.part AS part
    ON _t.l_partkey = part.p_partkey
  WHERE
    _t._w = 1
)
SELECT
  region.r_name AS region_name,
  _s9.p_name AS part_name
FROM tpch.region AS region
LEFT JOIN _s9 AS _s9
  ON _s9.n_regionkey = region.r_regionkey
ORDER BY
  1
