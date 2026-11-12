WITH _s0 AS (
  SELECT
    p_name,
    p_partkey
  FROM snowflake_sample_data.tpch_sf1.part
  ORDER BY
    2 NULLS FIRST
  LIMIT 3
), _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM part AS p
  CROSS JOIN LATERAL SPLIT_TO_TABLE(p.p_name, ' ') AS l
)
SELECT
  ANY_VALUE(_s0.p_name) AS name,
  MIN(_0.color) AS a,
  MAX(_0.color) AS b
FROM _s0 AS _s0
JOIN _0 AS _0
  ON CONTAINS(_s0.p_name, CONCAT_WS('', ' ', _0.color, ' '))
  OR ENDSWITH(_s0.p_name, CONCAT_WS('', ' ', _0.color))
  OR STARTSWITH(_s0.p_name, CONCAT_WS('', _0.color, ' '))
GROUP BY
  _s0.p_partkey
ORDER BY
  _s0.p_partkey NULLS FIRST
