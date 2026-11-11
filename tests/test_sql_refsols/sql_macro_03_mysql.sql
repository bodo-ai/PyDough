WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM part AS p
  CROSS JOIN JSON_TABLE(CONCAT('["', REPLACE(p.p_name, ' ', '","'), '"]'), '$[*]' COLUMNS(value VARCHAR(255) PATH '$')) AS l
)
SELECT
  COUNT(*) AS n
FROM _0
