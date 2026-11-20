WITH _0 AS (
  SELECT DISTINCT
    EXPLODE(SPLIT(p_name, ' ')) AS color
  FROM part
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
JOIN tpch_s3_pq.tpch_sf1.part AS part
  ON CONTAINS(
    part.p_name,
    CASE WHEN _s0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _s0.color, ' ') END
  )
  OR ENDSWITH(
    part.p_name,
    CASE WHEN _s0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _s0.color) END
  )
  OR STARTSWITH(
    part.p_name,
    CASE WHEN _s0.color IS NULL THEN NULL ELSE CONCAT_WS('', _s0.color, ' ') END
  )
GROUP BY
  1
ORDER BY
  1
