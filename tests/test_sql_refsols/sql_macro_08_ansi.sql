WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM part AS p
  CROSS JOIN JSON_EACH('["' || REPLACE(p.p_name, ' ', '","') || '"]') AS l
), _s0 AS (
  SELECT
    color
  FROM _0
  ORDER BY
    1
  LIMIT 5
)
SELECT
  _s0.color,
  COUNT(*) AS n_parts
FROM _s0 AS _s0
JOIN tpch.part AS part
  ON part.p_name LIKE CONCAT('%', CONCAT_WS('', ' ', _s0.color, ' '), '%')
  OR part.p_name LIKE CONCAT('%', CONCAT_WS('', ' ', _s0.color))
  OR part.p_name LIKE CONCAT(CONCAT_WS('', _s0.color, ' '), '%')
GROUP BY
  1
ORDER BY
  1
