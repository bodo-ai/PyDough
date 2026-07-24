SELECT
  MIN(n_regionkey) AS min_region
FROM tpch.nation
WHERE
  n_name LIKE 'I%'
