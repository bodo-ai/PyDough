WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM part AS p
  CROSS JOIN JSON_TABLE(CONCAT('["', REPLACE(p.p_name, ' ', '","'), '"]'), '$[*]' COLUMNS(value VARCHAR(255) PATH '$')) AS l
)
SELECT
  _0.color COLLATE utf8mb4_bin AS color,
  COUNT(*) AS n
FROM _0 AS _0
JOIN tpch.PART AS PART
  ON PART.p_name LIKE CONCAT('%', CONCAT_WS('', ' ', _0.color, ' '), '%')
  OR PART.p_name LIKE CONCAT('%', CONCAT_WS('', ' ', _0.color))
  OR PART.p_name LIKE CONCAT(CONCAT_WS('', _0.color, ' '), '%')
GROUP BY
  1
ORDER BY
  2 DESC,
  1
LIMIT 3
