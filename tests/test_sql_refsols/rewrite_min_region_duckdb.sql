SELECT
  MIN(n_regionkey) AS min_region
FROM main.nation
WHERE
  n_name LIKE 'I%'
