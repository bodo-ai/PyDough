WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM mysql.tpch.part AS p
  CROSS JOIN UNNEST(SPLIT(p.p_name, ' ')) AS l(value)
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
JOIN mysql.tpch.part AS part
  ON STARTS_WITH(
    part.p_name,
    CASE WHEN _s0.color IS NULL THEN NULL ELSE CONCAT_WS('', _s0.color, ' ') END
  )
  OR part.p_name LIKE CONCAT(
    '%',
    CASE WHEN _s0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _s0.color, ' ') END,
    '%'
  )
  OR part.p_name LIKE CONCAT('%', CASE WHEN _s0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _s0.color) END)
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
