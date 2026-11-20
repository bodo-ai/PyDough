WITH _0 AS (
  SELECT DISTINCT
    EXPLODE(SPLIT(p_name, ' ')) AS color
  FROM part
)
SELECT
  COUNT(*) AS n
FROM _0
