WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM mysql.tpch.part AS p
  CROSS JOIN UNNEST(SPLIT(p.p_name, ' ')) AS l(value)
)
SELECT
  color
FROM _0
ORDER BY
  1 NULLS FIRST
LIMIT 5
