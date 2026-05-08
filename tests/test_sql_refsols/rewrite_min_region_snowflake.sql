SELECT
  MIN(n_regionkey) AS min_region
FROM tpch.nation
WHERE
  STARTSWITH(n_name, 'I')
