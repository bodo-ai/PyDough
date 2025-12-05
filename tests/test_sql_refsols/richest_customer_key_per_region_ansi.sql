WITH _t0 AS (
  SELECT
    customer.c_custkey
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY n_regionkey ORDER BY customer.c_acctbal DESC NULLS FIRST) = 1
)
SELECT
  c_custkey AS key
FROM _t0
