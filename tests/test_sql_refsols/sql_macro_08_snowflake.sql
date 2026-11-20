WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM part AS p
  CROSS JOIN LATERAL SPLIT_TO_TABLE(p.p_name, ' ') AS l
), _s0 AS (
  SELECT
    color
  FROM _0
  ORDER BY
    1 NULLS FIRST
  LIMIT 5
)
SELECT
  _s0.color,
  COUNT(*) AS n_parts
FROM _s0 AS _s0
JOIN snowflake_sample_data.tpch_sf1.part AS part
  ON CONTAINS(part.p_name, CONCAT_WS('', ' ', _s0.color, ' '))
  OR ENDSWITH(part.p_name, CONCAT_WS('', ' ', _s0.color))
  OR STARTSWITH(part.p_name, CONCAT_WS('', _s0.color, ' '))
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
