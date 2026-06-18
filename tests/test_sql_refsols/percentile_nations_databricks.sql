SELECT
  n_name AS name,
  NTILE(5) OVER (ORDER BY n_name NULLS LAST) AS p1,
  NTILE(5) OVER (ORDER BY n_name NULLS LAST) AS p2
FROM tpch.nation
