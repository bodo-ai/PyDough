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
), _t0 AS (
  SELECT
    _s0.color,
    part.p_partkey,
    part.p_retailprice
  FROM _s0 AS _s0
  JOIN snowflake_sample_data.tpch_sf1.part AS part
    ON CONTAINS(part.p_name, CONCAT_WS('', ' ', _s0.color, ' '))
    OR ENDSWITH(part.p_name, CONCAT_WS('', ' ', _s0.color))
    OR STARTSWITH(part.p_name, CONCAT_WS('', _s0.color, ' '))
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY _s0.color ORDER BY part.p_retailprice DESC, part.p_partkey) = 1
)
SELECT
  color,
  p_partkey AS key,
  p_retailprice AS retail_price
FROM _t0
