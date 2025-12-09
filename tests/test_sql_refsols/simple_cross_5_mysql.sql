WITH _t1 AS (
  SELECT
    p_container,
    p_size
  FROM tpch.PART
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
    ORDERS.o_orderpriority,
    _s0.p_size,
    SUM(LINEITEM.l_quantity) AS sum_l_quantity
  FROM _s0 AS _s0
  JOIN tpch.ORDERS AS ORDERS
    ON EXTRACT(MONTH FROM CAST(ORDERS.o_orderdate AS DATETIME)) = 1
    AND EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) = 1998
  JOIN tpch.LINEITEM AS LINEITEM
    ON LINEITEM.l_discount = 0
    AND LINEITEM.l_orderkey = ORDERS.o_orderkey
    AND LINEITEM.l_shipmode = 'SHIP'
    AND LINEITEM.l_tax = 0
  JOIN tpch.PART AS PART
    ON LINEITEM.l_partkey = PART.p_partkey
    AND PART.p_container LIKE 'LG%'
    AND PART.p_size = _s0.p_size
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
