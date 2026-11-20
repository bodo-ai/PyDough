WITH _t0 AS (
  SELECT
    customer.c_custkey
  FROM snowflake_sample_data.tpch_sf1.nation AS nation
  JOIN snowflake_sample_data.tpch_sf1.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY nation.n_regionkey ORDER BY customer.c_acctbal DESC) = 1
)
SELECT
  c_custkey AS key
FROM _t0
