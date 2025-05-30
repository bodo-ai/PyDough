WITH _t0 AS (
  SELECT
    AVG(nation.n_nationkey) OVER (PARTITION BY region.r_regionkey) AS key_avg,
    SUM(nation.n_nationkey) OVER (PARTITION BY region.r_regionkey) AS key_sum,
    COUNT(*) OVER (PARTITION BY region.r_regionkey) AS n_nations,
    COUNT(CASE WHEN LENGTH(nation.n_comment) < 75 THEN nation.n_comment ELSE NULL END) OVER (PARTITION BY region.r_regionkey) AS n_short_comment,
    nation.n_name AS nation_name,
    nation.n_regionkey AS region_key
  FROM tpch.region AS region
  JOIN tpch.nation AS nation
    ON NOT SUBSTRING(nation.n_name, 1, 1) IN ('A', 'E', 'I', 'O', 'U')
    AND nation.n_regionkey = region.r_regionkey
)
SELECT
  nation_name,
  key_sum,
  key_avg,
  n_short_comment,
  n_nations
FROM _t0
ORDER BY
  region_key,
  nation_name
