WITH _t0 AS (
  SELECT
    NTH_VALUE(nation.n_name, 3) OVER (ORDER BY nation.n_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS v1,
    NTH_VALUE(nation.n_name, 1) OVER (PARTITION BY nation.n_regionkey ORDER BY nation.n_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS v2,
    NTH_VALUE(nation.n_name, 2) OVER (PARTITION BY nation.n_regionkey ORDER BY nation.n_name ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS v3,
    NTH_VALUE(nation.n_name, 5) OVER (ORDER BY nation.n_name ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS v4,
    nation.n_name,
    region_2.r_name
  FROM tpch.region AS region
  JOIN tpch.nation AS nation
    ON nation.n_regionkey = region.r_regionkey
  JOIN tpch.region AS region_2
    ON nation.n_regionkey = region_2.r_regionkey
)
SELECT
  r_name AS rname,
  n_name AS nname,
  v1,
  v2,
  v3,
  v4
FROM _t0
ORDER BY
  r_name,
  n_name
