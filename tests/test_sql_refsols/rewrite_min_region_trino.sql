SELECT
  MIN(n_regionkey) AS min_region
FROM tpch.nation
WHERE
  STARTS_WITH(n_name, 'I')
