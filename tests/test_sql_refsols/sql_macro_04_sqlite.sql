WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM part AS p
  CROSS JOIN JSON_EACH('["' || REPLACE(p.p_name, ' ', '","') || '"]') AS l
)
SELECT
  _0.color,
  COUNT(*) AS n
FROM _0 AS _0
JOIN tpch.part AS part
  ON part.p_name LIKE (
    '%' || CONCAT_WS('', ' ', _0.color)
  )
  OR part.p_name LIKE (
    CONCAT_WS('', _0.color, ' ') || '%'
  )
  OR part.p_name LIKE (
    '%' || CONCAT_WS('', ' ', _0.color, ' ') || '%'
  )
GROUP BY
  1
ORDER BY
  2 DESC,
  1
LIMIT 3
