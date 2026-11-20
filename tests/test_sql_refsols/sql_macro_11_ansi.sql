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
), _s1 AS (
  SELECT
    o_orderpriority,
    SUM(o_totalprice) AS sum_o_totalprice
  FROM tpch.orders
  GROUP BY
    1
)
SELECT
  _0.priority_value AS prio,
  SUM(COALESCE(_s1.sum_o_totalprice, 0)) OVER (
    ORDER BY _0.priority_name DESC NULLS FIRST
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cum_val
FROM _0 AS _0
JOIN _s1 AS _s1
  ON _0.priority_name = _s1.o_orderpriority
