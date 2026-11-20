WITH _0 AS (
  SELECT DISTINCT
    UNNEST(STRING_TO_ARRAY(p_name, ' ')) AS color
  FROM tpch.part
), _s0 AS (
  SELECT
    color
  FROM _0
  ORDER BY
    1 NULLS FIRST
  LIMIT 5
)
SELECT
  _s0.color,
  COUNT(*) AS n_parts
FROM _s0 AS _s0
JOIN tpch.part AS part
  ON part.p_name LIKE (
    '%' || CASE WHEN _s0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _s0.color) END
  )
  OR part.p_name LIKE (
    CASE WHEN _s0.color IS NULL THEN NULL ELSE CONCAT_WS('', _s0.color, ' ') END || '%'
  )
  OR part.p_name LIKE (
    '%' || CASE WHEN _s0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _s0.color, ' ') END || '%'
  )
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
