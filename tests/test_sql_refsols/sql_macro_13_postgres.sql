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
), _1 AS (
  SELECT DISTINCT
    UNNEST(STRING_TO_ARRAY(p_name, ' ')) AS color
  FROM tpch.part
), _u_0 AS (
  SELECT
    _3.color AS _u_1,
    _2.priority_name AS _u_2
  FROM _0 AS _2
  CROSS JOIN _1 AS _3
  JOIN tpch.part AS part
    ON (
      part.p_name LIKE (
        '%' || CASE WHEN _3.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _3.color) END
      )
      OR part.p_name LIKE (
        CASE WHEN _3.color IS NULL THEN NULL ELSE CONCAT_WS('', _3.color, ' ') END || '%'
      )
      OR part.p_name LIKE (
        '%' || CASE WHEN _3.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _3.color, ' ') END || '%'
      )
    )
    AND part.p_size < 3
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_partkey = part.p_partkey
    AND lineitem.l_shipdate < CAST('1992-02-01' AS DATE)
    AND lineitem.l_shipmode = 'RAIL'
  JOIN tpch.orders AS orders
    ON _2.priority_name = orders.o_orderpriority
    AND lineitem.l_orderkey = orders.o_orderkey
    AND orders.o_clerk < 'Clerk#000000100'
    AND orders.o_orderdate < CAST('1992-02-01' AS DATE)
  GROUP BY
    1,
    2
)
SELECT
  COUNT(*) AS n
FROM _0 AS _0
CROSS JOIN _1 AS _1
LEFT JOIN _u_0 AS _u_0
  ON _0.priority_name = _u_0._u_2 AND _1.color = _u_0._u_1
WHERE
  _u_0._u_1 IS NULL
