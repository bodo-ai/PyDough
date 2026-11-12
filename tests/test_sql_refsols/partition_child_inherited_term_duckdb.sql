SELECT
  region.r_regionkey AS rkey,
  MAX(region.r_name) AS region_name,
  COUNT(*) AS n_nations
FROM main.region AS region
JOIN main.nation AS nation
  ON nation.n_regionkey = region.r_regionkey
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
