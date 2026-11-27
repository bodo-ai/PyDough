SELECT
  CONCAT_WS(' - ', CUSTOMER.c_mktsegment, NATION.n_name) COLLATE utf8mb4_bin AS label,
  COUNT(*) AS n
FROM tpch.CUSTOMER AS CUSTOMER
JOIN tpch.NATION AS NATION
  ON CUSTOMER.c_nationkey = NATION.n_nationkey
GROUP BY
  1
ORDER BY
  1
