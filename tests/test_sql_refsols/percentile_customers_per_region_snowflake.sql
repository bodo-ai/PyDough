WITH _t1 AS (
  SELECT
    customer.c_name
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  QUALIFY
    ENDSWITH(customer.c_phone, '00')
    AND NTILE(100) OVER (PARTITION BY nation.n_regionkey ORDER BY customer.c_acctbal) = 95
)
SELECT
  c_name AS name
FROM _t1
ORDER BY
  1 NULLS FIRST
