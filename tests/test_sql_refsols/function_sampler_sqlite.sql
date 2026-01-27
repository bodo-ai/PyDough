SELECT
  CONCAT_WS('-', region.r_name, nation.n_name, SUBSTRING(customer.c_name, 17)) AS a,
  ROUND(customer.c_acctbal, 1) AS b,
  CASE WHEN SUBSTRING(customer.c_phone, 1, 1) = '3' THEN customer.c_name ELSE NULL END AS c,
  NOT CASE WHEN SUBSTRING(customer.c_phone, 2, 1) = '1' THEN customer.c_name ELSE NULL END IS NULL AS d,
  CASE WHEN SUBSTRING(customer.c_phone, 15) = '7' THEN customer.c_name ELSE NULL END IS NULL AS e,
  ROUND(customer.c_acctbal, 0) AS f
FROM tpch.region AS region
JOIN tpch.nation AS nation
  ON nation.n_regionkey = region.r_regionkey
JOIN tpch.customer AS customer
  ON customer.c_acctbal <= 100.0
  AND customer.c_acctbal >= 0.0
  AND customer.c_nationkey = nation.n_nationkey
ORDER BY
  customer.c_address
LIMIT 10
