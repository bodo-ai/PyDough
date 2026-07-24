SELECT
  MIN(n_regionkey) AS min_region
FROM TPCH.NATION
WHERE
  n_name LIKE 'I%'
