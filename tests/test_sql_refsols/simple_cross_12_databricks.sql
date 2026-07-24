WITH _s0 AS (
  SELECT DISTINCT
    o_orderpriority
  FROM tpch.orders
), _s1 AS (
  SELECT DISTINCT
    c_mktsegment
  FROM tpch.customer
)
SELECT
  _s0.o_orderpriority AS order_priority,
  _s1.c_mktsegment AS market_segment
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
ORDER BY
  1,
  2
