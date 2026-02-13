SELECT
  region.r_name AS rname,
  nation.n_name AS nname,
  NTH_VALUE(nation.n_name, 3) OVER (ORDER BY nation.n_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS v1,
  NTH_VALUE(nation.n_name, 1) OVER (PARTITION BY nation.n_regionkey ORDER BY nation.n_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS v2,
  NTH_VALUE(nation.n_name, 2) OVER (PARTITION BY nation.n_regionkey ORDER BY nation.n_name ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS v3,
  NTH_VALUE(nation.n_name, 5) OVER (ORDER BY nation.n_name ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS v4
FROM tpch.region AS region
JOIN tpch.nation AS nation
  ON nation.n_regionkey = region.r_regionkey
ORDER BY
  1,
  2
