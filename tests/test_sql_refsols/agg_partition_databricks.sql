WITH _t0 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM tpch_s3_pq.tpch_sf1.orders
  GROUP BY
    EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP))
)
SELECT
  MAX(n_rows) AS best_year
FROM _t0
