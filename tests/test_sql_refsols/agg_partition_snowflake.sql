WITH _t0 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM snowflake_sample_data.tpch_sf1.orders
  GROUP BY
    YEAR(CAST(o_orderdate AS TIMESTAMP))
)
SELECT
  MAX(n_rows) AS best_year
FROM _t0
