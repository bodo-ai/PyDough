WITH _0 AS (
  SELECT DISTINCT
    EXPLODE(SPLIT(p_name, ' ')) AS color
  FROM part
), _s0 AS (
  SELECT
    color
  FROM _0
  ORDER BY
    1
  LIMIT 5
), _t0 AS (
  SELECT
    _s0.color,
    part.p_partkey,
    part.p_retailprice
  FROM _s0 AS _s0
  JOIN tpch_s3_pq.tpch_sf1.part AS part
    ON CONTAINS(
      part.p_name,
      CASE WHEN _s0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _s0.color, ' ') END
    )
    OR ENDSWITH(
      part.p_name,
      CASE WHEN _s0.color IS NULL THEN NULL ELSE CONCAT_WS('', ' ', _s0.color) END
    )
    OR STARTSWITH(
      part.p_name,
      CASE WHEN _s0.color IS NULL THEN NULL ELSE CONCAT_WS('', _s0.color, ' ') END
    )
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY _s0.color
      ORDER BY part.p_retailprice DESC NULLS FIRST, part.p_partkey NULLS LAST
    ) = 1
)
SELECT
  color,
  p_partkey AS key,
  p_retailprice AS retail_price
FROM _t0
ORDER BY
  1
