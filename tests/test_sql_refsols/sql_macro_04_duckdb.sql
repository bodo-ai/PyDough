WITH _0 AS (
  SELECT DISTINCT
    UNNEST(STR_SPLIT(p_name, ' ')) AS color
  FROM part
)
SELECT
  _0.color,
  COUNT(*) AS n
FROM _0 AS _0
JOIN main.part AS part
  ON part.p_name LIKE (
    '%' || CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _0.color) END
  )
  OR part.p_name LIKE (
    CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', _0.color, ' ') END || '%'
  )
  OR part.p_name LIKE (
    '%' || CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _0.color, ' ') END || '%'
  )
GROUP BY
  1
ORDER BY
  2 DESC,
  1 NULLS FIRST
LIMIT 3
