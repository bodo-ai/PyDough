SELECT
  MIN(n_regionkey) AS min_region
FROM snowflake_sample_data.tpch_sf1.nation
WHERE
  STARTSWITH(n_name, 'I')
