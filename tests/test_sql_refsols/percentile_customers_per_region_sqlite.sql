WITH _t AS (
  SELECT
    customer.c_name,
    customer.c_phone,
    NTILE(100) OVER (PARTITION BY nation.n_regionkey ORDER BY customer.c_acctbal) AS _w
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
)
SELECT
  c_name AS name
FROM _t
WHERE
  _w = 95 AND c_phone LIKE '%00'
ORDER BY
  1
