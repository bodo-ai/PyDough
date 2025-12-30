WITH _t1 AS (
  SELECT
    customer.c_name
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  QUALIFY
    NTILE(100) OVER (PARTITION BY nation.n_regionkey ORDER BY customer.c_acctbal NULLS LAST) = 95
    AND customer.c_phone LIKE '%00'
)
SELECT
  c_name AS name
FROM _t1
ORDER BY
  1
