SELECT
  COALESCE(SUM(o_totalprice), 0) AS total_1995
FROM snowflake_sample_data.tpch_sf1.orders
WHERE
  YEAR(CAST(o_orderdate AS TIMESTAMP)) = 1995
