SELECT
  COUNT(DISTINCT nation.n_nationkey) AS n
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA'
WHERE
  nation.n_name = 'CHINA'
