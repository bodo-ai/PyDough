WITH _s1 AS (
  SELECT
    n_name,
    n_regionkey
  FROM tpch.nation
)
SELECT
  MAX(region.r_name) AS region_name,
  COUNT(*) AS n_prefix_nations
FROM tpch.region AS region
LEFT JOIN _s1 AS _s1
  ON SUBSTRING(_s1.n_name, 1, 1) = SUBSTRING(region.r_name, 1, 1)
  AND _s1.n_regionkey = region.r_regionkey
GROUP BY
  _s1.n_regionkey,
  SUBSTRING(_s1.n_name, 1, 1)
ORDER BY
  1
