WITH _0 AS (
  SELECT DISTINCT
    EXPLODE(SPLIT(p_name, ' ')) AS color
  FROM part
)
SELECT
  color
FROM _0
ORDER BY
  1
LIMIT 5
