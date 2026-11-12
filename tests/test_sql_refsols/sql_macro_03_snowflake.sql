WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM part AS p
  CROSS JOIN LATERAL SPLIT_TO_TABLE(p.p_name, ' ') AS l
)
SELECT
  COUNT(*) AS n
FROM _0
