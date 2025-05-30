WITH _t0 AS (
  SELECT
    AVG(n_nationkey) OVER () AS key_avg,
    SUM(n_nationkey) OVER () AS key_sum,
    COUNT(*) OVER () AS n_nations,
    COUNT(CASE WHEN LENGTH(n_comment) < 75 THEN n_comment ELSE NULL END) OVER () AS n_short_comment,
    n_name AS nation_name,
    n_regionkey AS region_key
  FROM tpch.nation
  WHERE
    NOT SUBSTRING(n_name, 1, 1) IN ('A', 'E', 'I', 'O', 'U')
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
