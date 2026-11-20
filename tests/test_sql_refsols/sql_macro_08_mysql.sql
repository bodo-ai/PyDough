WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM tpch.PART AS p
  CROSS JOIN JSON_TABLE(CONCAT('["', REPLACE(p.p_name, ' ', '","'), '"]'), '$[*]' COLUMNS(value VARCHAR(255) PATH '$')) AS l
), _s0 AS (
  SELECT
    color COLLATE utf8mb4_bin AS color
  FROM _0
  ORDER BY
    1
  LIMIT 5
)
SELECT
  _s0.color COLLATE utf8mb4_bin AS color,
  COUNT(*) AS n_parts
FROM _s0 AS _s0
JOIN tpch.PART AS PART
  ON PART.p_name LIKE CONCAT('%', CONCAT_WS('', ' ', _s0.color, ' '), '%')
  OR PART.p_name LIKE CONCAT('%', CONCAT_WS('', ' ', _s0.color))
  OR PART.p_name LIKE CONCAT(CONCAT_WS('', _s0.color, ' '), '%')
GROUP BY
  1
ORDER BY
  1
