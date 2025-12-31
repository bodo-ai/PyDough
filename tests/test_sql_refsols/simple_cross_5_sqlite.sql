WITH _t1 AS (
  SELECT
    p_container,
    p_size
  FROM tpch.part
  WHERE
    p_container LIKE 'LG%'
), _s6 AS (
  SELECT DISTINCT
    p_size
  FROM _t1
  ORDER BY
    1
  LIMIT 10
), _s0 AS (
  SELECT DISTINCT
    p_size
  FROM _t1
  ORDER BY
    1
  LIMIT 10
), _t4 AS (
  SELECT
    orders.o_orderpriority,
    _s0.p_size,
    SUM(lineitem.l_quantity) AS sum_l_quantity
  FROM _s0 AS _s0
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1998
    AND CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) = 1
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_discount = 0
    AND lineitem.l_orderkey = orders.o_orderkey
    AND lineitem.l_shipmode = 'SHIP'
    AND lineitem.l_tax = 0
  JOIN tpch.part AS part
    ON _s0.p_size = part.p_size
    AND lineitem.l_partkey = part.p_partkey
    AND part.p_container LIKE 'LG%'
  GROUP BY
    1,
    2
), _t AS (
  SELECT
    o_orderpriority,
    p_size,
    sum_l_quantity,
    ROW_NUMBER() OVER (PARTITION BY p_size ORDER BY COALESCE(sum_l_quantity, 0) DESC) AS _w
  FROM _t4
), _s7 AS (
  SELECT
    COALESCE(sum_l_quantity, 0) AS total_qty,
    o_orderpriority,
    p_size
  FROM _t
  WHERE
    _w = 1
)
SELECT
  _s6.p_size AS part_size,
  _s7.o_orderpriority AS best_order_priority,
  _s7.total_qty AS best_order_priority_qty
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.p_size = _s7.p_size
ORDER BY
  1
