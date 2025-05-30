WITH _t6 AS (
  SELECT
    l_orderkey AS order_key,
    l_partkey AS part_key,
    l_quantity AS quantity,
    l_shipmode AS ship_mode
  FROM tpch.lineitem
  WHERE
    l_shipmode = 'RAIL'
), _s0 AS (
  SELECT
    SUM(quantity) AS agg_0,
    order_key,
    part_key
  FROM _t6
  GROUP BY
    order_key,
    part_key
), _t3 AS (
  SELECT
    SUM(_s0.agg_0) AS agg_0,
    _s0.part_key
  FROM _s0 AS _s0
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1995
    AND _s0.order_key = orders.o_orderkey
  GROUP BY
    _s0.part_key
), _s4 AS (
  SELECT
    SUM(quantity) AS agg_1,
    order_key,
    part_key
  FROM _t6
  GROUP BY
    order_key,
    part_key
), _t8 AS (
  SELECT
    SUM(_s4.agg_1) AS agg_1,
    _s4.part_key
  FROM _s4 AS _s4
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1996
    AND _s4.order_key = orders.o_orderkey
  GROUP BY
    _s4.part_key
)
SELECT
  part.p_name AS name,
  COALESCE(_t3.agg_0, 0) AS qty_95,
  COALESCE(_t8.agg_1, 0) AS qty_96
FROM tpch.part AS part
JOIN _t3 AS _t3
  ON _t3.part_key = part.p_partkey
JOIN _t8 AS _t8
  ON _t8.part_key = part.p_partkey
WHERE
  part.p_container LIKE 'SM%'
ORDER BY
  COALESCE(_t8.agg_1, 0) - COALESCE(_t3.agg_0, 0) DESC,
  name
LIMIT 3
