WITH _s0 AS (
  SELECT
    p_name,
    p_partkey
  FROM tpch.PART
  ORDER BY
    2
  LIMIT 3
), _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM tpch.PART AS p
  CROSS JOIN JSON_TABLE(CONCAT('["', REPLACE(p.p_name, ' ', '","'), '"]'), '$[*]' COLUMNS(value VARCHAR(255) PATH '$')) AS l
)
SELECT
  ANY_VALUE(_s0.p_name) AS name,
  MIN(_0.color) AS a,
  MAX(_0.color) AS b
FROM _s0 AS _s0
JOIN _0 AS _0
  ON _s0.p_name LIKE CONCAT('%', CONCAT_WS('', ' ', _0.color, ' '), '%')
  OR _s0.p_name LIKE CONCAT('%', CONCAT_WS('', ' ', _0.color))
  OR _s0.p_name LIKE CONCAT(CONCAT_WS('', _0.color, ' '), '%')
GROUP BY
  _s0.p_partkey
ORDER BY
  _s0.p_partkey
