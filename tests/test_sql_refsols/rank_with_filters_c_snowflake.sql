WITH _s0 AS (
  SELECT DISTINCT
    p_size
  FROM tpch.part
  ORDER BY
    1 DESC NULLS LAST
  LIMIT 5
), _t0 AS (
  SELECT
    part.p_size AS size_1,
    part.p_name
  FROM _s0 AS _s0
  JOIN tpch.part AS part
    ON _s0.p_size = part.p_size
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY p_size ORDER BY part.p_retailprice DESC, part.p_partkey) = 1
)
SELECT
  p_name AS pname,
  size_1 AS psize
FROM _t0
