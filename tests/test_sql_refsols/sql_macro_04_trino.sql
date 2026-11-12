WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM mysql.tpch.part AS p
  CROSS JOIN UNNEST(SPLIT(p.p_name, ' ')) AS l(value)
)
SELECT
  _0.color,
  COUNT(*) AS n
FROM _0 AS _0
JOIN mysql.tpch.part AS part
  ON STARTS_WITH(
    part.p_name,
    CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', _0.color, ' ') END
  )
  OR part.p_name LIKE CONCAT(
    '%',
    CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _0.color, ' ') END,
    '%'
  )
  OR part.p_name LIKE CONCAT('%', CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _0.color) END)
GROUP BY
  1
ORDER BY
  2 DESC,
  1 NULLS FIRST
LIMIT 3
