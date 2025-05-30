WITH _s0 AS (
  SELECT DISTINCT
    p_size AS size
  FROM tpch.part
  ORDER BY
    size DESC
  LIMIT 5
), _t0 AS (
  SELECT
    part.p_name AS name,
    part.p_size AS size_3
  FROM _s0 AS _s0
  JOIN tpch.part AS part
    ON _s0.size = part.p_size
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY _s0.size ORDER BY part.p_retailprice DESC NULLS FIRST) = 1
)
SELECT
  name AS pname,
  size_3 AS psize
FROM _t0
