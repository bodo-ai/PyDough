WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM tpch.PART AS p
  CROSS JOIN JSON_TABLE(CONCAT('["', REPLACE(p.p_name, ' ', '","'), '"]'), '$[*]' COLUMNS(value VARCHAR(255) PATH '$')) AS l
)
SELECT
  color COLLATE utf8mb4_bin AS color
FROM _0
ORDER BY
  1
LIMIT 5
