WITH _s0 AS (
  SELECT
    n_name AS nation_name,
    n_name AS name,
    n_regionkey AS region_key
  FROM tpch.nation
  ORDER BY
    name
  LIMIT 2
)
SELECT
  _s0.nation_name,
  region.r_name AS a1,
  region.r_name AS a2,
  COALESCE(region.r_regionkey, 0) AS a3,
  CASE
    WHEN NOT CASE WHEN region.r_name <> 'AMERICA' THEN region.r_regionkey ELSE NULL END IS NULL
    THEN 1
    ELSE 0
  END AS a4,
  1 AS a5,
  region.r_regionkey AS a6,
  region.r_name AS a7,
  region.r_regionkey AS a8
FROM _s0 AS _s0
JOIN tpch.region AS region
  ON _s0.region_key = region.r_regionkey
ORDER BY
  _s0.name
