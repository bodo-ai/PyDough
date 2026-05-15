WITH _s0 AS (
  SELECT
    n_name,
    n_regionkey
  FROM tpch.nation
  ORDER BY
    1 NULLS FIRST
  LIMIT 2
)
SELECT
  _s0.n_name AS nation_name,
  region.r_name AS a1,
  region.r_name AS a2,
  region.r_regionkey AS a3,
  IFF(
    NOT CASE WHEN region.r_name <> 'AMERICA' THEN region.r_regionkey ELSE NULL END IS NULL,
    1,
    0
  ) AS a4,
  1 AS a5,
  region.r_regionkey AS a6,
  region.r_name AS a7,
  region.r_regionkey AS a8
FROM _s0 AS _s0
JOIN tpch.region AS region
  ON _s0.n_regionkey = region.r_regionkey
ORDER BY
  1 NULLS FIRST
