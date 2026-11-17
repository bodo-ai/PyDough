WITH _0 AS (
  SELECT
    '1-URGENT' AS priority_name,
    1 AS priority_value
  UNION ALL
  SELECT
    '2-HIGH' AS `2-HIGH`,
    2 AS `2`
  UNION ALL
  SELECT
    '3-MEDIUM' AS `3-MEDIUM`,
    3 AS `3`
  UNION ALL
  SELECT
    '4-NOT SPECIFIED' AS `4-NOT SPECIFIED`,
    4 AS `4`
  UNION ALL
  SELECT
    '5-LOW' AS `5-LOW`,
    5 AS `5`
), _1 AS (
  SELECT DISTINCT
    l.value AS color
  FROM tpch.PART AS p
  CROSS JOIN JSON_TABLE(CONCAT('["', REPLACE(p.p_name, ' ', '","'), '"]'), '$[*]' COLUMNS(value VARCHAR(255) PATH '$')) AS l
), _u_0 AS (
  SELECT
    _3.color AS _u_1,
    _2.priority_name AS _u_2
  FROM _0 AS _2
  CROSS JOIN _1 AS _3
  JOIN tpch.PART AS PART
    ON (
      PART.p_name LIKE CONCAT('%', CONCAT_WS('', ' ', _3.color, ' '), '%')
      OR PART.p_name LIKE CONCAT('%', CONCAT_WS('', ' ', _3.color))
      OR PART.p_name LIKE CONCAT(CONCAT_WS('', _3.color, ' '), '%')
    )
    AND PART.p_size < 3
  JOIN tpch.LINEITEM AS LINEITEM
    ON LINEITEM.l_partkey = PART.p_partkey
    AND LINEITEM.l_shipdate < CAST('1992-02-01' AS DATE)
    AND LINEITEM.l_shipmode = 'RAIL'
  JOIN tpch.ORDERS AS ORDERS
    ON LINEITEM.l_orderkey = ORDERS.o_orderkey
    AND ORDERS.o_clerk < 'Clerk#000000100'
    AND ORDERS.o_orderdate < CAST('1992-02-01' AS DATE)
    AND ORDERS.o_orderpriority = _2.priority_name
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
  NOT NOT _u_0._u_1 IS NULL
