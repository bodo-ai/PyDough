WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM part AS p
  CROSS JOIN LATERAL SPLIT_TO_TABLE(p.p_name, ' ') AS l
)
SELECT
  _0.color,
  COUNT(*) AS n
FROM _0 AS _0
JOIN snowflake_sample_data.tpch_sf1.part AS part
  ON CONTAINS(part.p_name, CONCAT_WS('', ' ', _0.color, ' '))
  OR ENDSWITH(part.p_name, CONCAT_WS('', ' ', _0.color))
  OR STARTSWITH(part.p_name, CONCAT_WS('', _0.color, ' '))
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 3
