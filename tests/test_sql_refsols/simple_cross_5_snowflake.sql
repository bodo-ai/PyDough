WITH _t1 AS (
  SELECT
    p_container,
    p_size
  FROM tpch.part
  WHERE
    STARTSWITH(p_container, 'LG')
), _s6 AS (
  SELECT DISTINCT
    p_size
  FROM _t1
  ORDER BY
    1 NULLS FIRST
  LIMIT 10
), _s0 AS (
  SELECT DISTINCT
    p_size
  FROM _t1
  ORDER BY
    1 NULLS FIRST
  LIMIT 10
), _t3 AS (
  SELECT
    orders.o_orderpriority,
    _s0.p_size,
    SUM(lineitem.l_quantity) AS sum_l_quantity
  FROM _s0 AS _s0
  JOIN tpch.orders AS orders
    ON MONTH(CAST(orders.o_orderdate AS TIMESTAMP)) = 1
    AND YEAR(CAST(orders.o_orderdate AS TIMESTAMP)) = 1998
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_discount = 0
    AND lineitem.l_orderkey = orders.o_orderkey
    AND lineitem.l_shipmode = 'SHIP'
    AND lineitem.l_tax = 0
  JOIN tpch.part AS part
    ON STARTSWITH(part.p_container, 'LG')
    AND _s0.p_size = part.p_size
    AND lineitem.l_partkey = part.p_partkey
  GROUP BY
    1,
    2
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY p_size ORDER BY COALESCE(SUM(lineitem.l_quantity), 0) DESC) = 1
)
SELECT
  _s6.p_size AS part_size,
  _t3.o_orderpriority AS best_order_priority,
  COALESCE(_t3.sum_l_quantity, 0) AS best_order_priority_qty
FROM _s6 AS _s6
LEFT JOIN _t3 AS _t3
  ON _s6.p_size = _t3.p_size
ORDER BY
  1 NULLS FIRST
