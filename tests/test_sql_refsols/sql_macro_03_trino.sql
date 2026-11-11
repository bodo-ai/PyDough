WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM part AS p
  CROSS JOIN UNNEST(SPLIT(p.p_name, ' ')) AS l
)
SELECT
  COUNT(*) AS n
FROM _0
