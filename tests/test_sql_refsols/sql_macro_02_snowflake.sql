WITH _0 AS (
  SELECT
    '1-URGENT' AS priority_name,
    1 AS priority_value
  UNION ALL
  SELECT
    '2-HIGH' AS "2-HIGH",
    2 AS "2"
  UNION ALL
  SELECT
    '3-MEDIUM' AS "3-MEDIUM",
    3 AS "3"
  UNION ALL
  SELECT
    '4-NOT SPECIFIED' AS "4-NOT SPECIFIED",
    4 AS "4"
  UNION ALL
  SELECT
    '5-LOW' AS "5-LOW",
    5 AS "5"
), _s0 AS (
  SELECT
    priority_name,
    priority_value
  FROM _0
  ORDER BY
    2 NULLS FIRST
  LIMIT 3
)
SELECT
  orders.o_orderpriority AS name,
  COUNT(*) AS n_orders
FROM _s0 AS _s0
JOIN snowflake_sample_data.tpch_sf1.orders AS orders
  ON _s0.priority_name = orders.o_orderpriority
GROUP BY
  1
ORDER BY
  ANY_VALUE(_s0.priority_value) NULLS FIRST
