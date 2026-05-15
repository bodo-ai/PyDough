SELECT
  n_name AS nation_name,
  SUM(n_nationkey) OVER (PARTITION BY n_regionkey) AS key_sum,
  AVG(CAST(n_nationkey AS REAL)) OVER (PARTITION BY n_regionkey) AS key_avg,
  COUNT(CASE WHEN LENGTH(n_comment) < 75 THEN n_comment ELSE NULL END) OVER (PARTITION BY n_regionkey) AS n_short_comment,
  COUNT(*) OVER (PARTITION BY n_regionkey) AS n_nations
FROM tpch.nation
WHERE
  NOT SUBSTRING(n_name, 1, 1) IN ('A', 'E', 'I', 'O', 'U')
ORDER BY
  n_regionkey,
  1
