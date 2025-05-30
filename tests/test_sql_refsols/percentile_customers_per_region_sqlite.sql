WITH _t AS (
  SELECT
    customer.c_name AS name_8,
    customer.c_phone AS phone,
    NTILE(100) OVER (PARTITION BY region.r_regionkey ORDER BY customer.c_acctbal) AS _w
  FROM tpch.region AS region
  JOIN tpch.nation AS nation
    ON nation.n_regionkey = region.r_regionkey
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
)
SELECT
  name_8 AS name
FROM _t
WHERE
  _w = 95 AND phone LIKE '%00'
ORDER BY
  name_8
