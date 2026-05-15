WITH _t4 AS (
  SELECT
    l_orderkey,
    l_partkey,
    l_quantity,
    l_shipmode
  FROM tpch.lineitem
  WHERE
    l_shipmode = 'RAIL'
), _s6 AS (
  SELECT
    _t4.l_partkey,
    ANY_VALUE(part.p_name) AS anything_p_name,
    SUM(_t4.l_quantity) AS sum_l_quantity
  FROM tpch.part AS part
  JOIN _t4 AS _t4
    ON _t4.l_partkey = part.p_partkey
  JOIN tpch.orders AS orders
    ON YEAR(CAST(orders.o_orderdate AS TIMESTAMP)) = 1995
    AND _t4.l_orderkey = orders.o_orderkey
  WHERE
    STARTSWITH(part.p_container, 'SM')
  GROUP BY
    1
)
SELECT
  ANY_VALUE(_s6.anything_p_name) AS name,
  COALESCE(ANY_VALUE(_s6.sum_l_quantity), 0) AS qty_95,
  COALESCE(SUM(_t6.l_quantity), 0) AS qty_96
FROM _s6 AS _s6
JOIN _t4 AS _t6
  ON _s6.l_partkey = _t6.l_partkey
JOIN tpch.orders AS orders
  ON YEAR(CAST(orders.o_orderdate AS TIMESTAMP)) = 1996
  AND _t6.l_orderkey = orders.o_orderkey
GROUP BY
  _t6.l_partkey
ORDER BY
  COALESCE(SUM(_t6.l_quantity), 0) - COALESCE(ANY_VALUE(_s6.sum_l_quantity), 0) DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 3
