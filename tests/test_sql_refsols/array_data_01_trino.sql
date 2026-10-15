WITH _s1 AS (
  SELECT
    n_regionkey,
    ARRAY_AGG(n_name) AS listof_n_name
  FROM tpch.nation
  GROUP BY
    1
)
SELECT
  region.r_name AS region_name,
  _s1.listof_n_name AS nation_names
FROM tpch.region AS region
JOIN _s1 AS _s1
  ON _s1.n_regionkey = region.r_regionkey
ORDER BY
  1 NULLS FIRST
