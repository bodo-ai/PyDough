WITH _s0 AS (
  SELECT
    AVG(c_acctbal) AS avg_c_acctbal
  FROM tpch.customer
  WHERE
    SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND c_acctbal > 0.0
)
SELECT
  SUBSTRING(customer.c_phone, 1, 2) AS CNTRY_CODE,
  COUNT(*) AS NUM_CUSTS,
  COALESCE(SUM(customer.c_acctbal), 0) AS TOTACCTBAL
FROM _s0 AS _s0
JOIN tpch.customer AS customer
  ON SUBSTRING(customer.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
  AND _s0.avg_c_acctbal < customer.c_acctbal
JOIN tpch.orders AS orders
  ON customer.c_custkey = orders.o_custkey
GROUP BY
  1
ORDER BY
  1
