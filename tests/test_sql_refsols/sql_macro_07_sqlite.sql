WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM part AS p
  CROSS JOIN JSON_EACH('["' || REPLACE(p.p_name, ' ', '","') || '"]') AS l
)
SELECT
  color
FROM _0
ORDER BY
  1
LIMIT 5
