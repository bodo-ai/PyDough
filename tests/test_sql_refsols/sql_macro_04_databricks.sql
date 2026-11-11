WITH _0 AS (
  SELECT
    EXPLODE(SPLIT(p_name, ' ')) AS color
  FROM part
)
SELECT
  _0.color,
  COUNT(*) AS n
FROM _0 AS _0
JOIN tpch_s3_pq.tpch_sf1.part AS part
  ON CONTAINS(
    part.p_name,
    CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _0.color, ' ') END
  )
  OR ENDSWITH(
    part.p_name,
    CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _0.color) END
  )
  OR STARTSWITH(
    part.p_name,
    CASE WHEN _0.color IS NULL THEN NULL ELSE CONCAT_WS('', _0.color, ' ') END
  )
GROUP BY
  1
ORDER BY
  2 DESC,
  1
LIMIT 3
