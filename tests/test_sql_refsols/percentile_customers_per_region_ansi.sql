WITH _t1 AS (
  SELECT
    customer.c_name AS name_8
  FROM tpch.region AS region
  JOIN tpch.nation AS nation
    ON nation.n_regionkey = region.r_regionkey
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  QUALIFY
    NTILE(100) OVER (PARTITION BY region.r_regionkey ORDER BY customer.c_acctbal NULLS LAST) = 95
    AND customer.c_phone LIKE '%00'
)
SELECT
  name_8 AS name
FROM _t1
ORDER BY
  name_8
