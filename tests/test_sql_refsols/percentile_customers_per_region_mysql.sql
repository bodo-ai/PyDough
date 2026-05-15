WITH _t AS (
  SELECT
    CUSTOMER.c_name,
    CUSTOMER.c_phone,
    NTILE(100) OVER (PARTITION BY NATION.n_regionkey ORDER BY CASE WHEN CUSTOMER.c_acctbal IS NULL THEN 1 ELSE 0 END, CUSTOMER.c_acctbal) AS _w
  FROM tpch.NATION AS NATION
  JOIN tpch.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_nationkey = NATION.n_nationkey
)
SELECT
  c_name COLLATE utf8mb4_bin AS name
FROM _t
WHERE
  _w = 95 AND c_phone LIKE '%00'
ORDER BY
  1
