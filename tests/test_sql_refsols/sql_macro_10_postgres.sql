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
), _t AS (
  SELECT
    o_orderpriority,
    NTILE(10) OVER (ORDER BY o_totalprice, o_orderkey) AS _w
  FROM tpch.orders
), _s1 AS (
  SELECT
    o_orderpriority,
    COUNT(*) AS n_rows
  FROM _t
  WHERE
    _w = 1
  GROUP BY
    1
)
SELECT
  _0.priority_name AS prio,
  COALESCE(_s1.n_rows, 0) AS n_chosen
FROM _0 AS _0
LEFT JOIN _s1 AS _s1
  ON _0.priority_name = _s1.o_orderpriority
