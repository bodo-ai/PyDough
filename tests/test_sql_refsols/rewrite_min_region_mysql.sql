SELECT
  MIN(n_regionkey) AS min_region
FROM tpch.NATION
WHERE
  n_name LIKE 'I%'
