WITH _s1 AS (
  SELECT
    n_nationkey
  FROM tpch.nation
)
SELECT
  COUNT(*) AS n
FROM tpch.customer AS customer
SEMI JOIN _s1 AS _s1
  ON _s1.n_nationkey = customer.c_nationkey
