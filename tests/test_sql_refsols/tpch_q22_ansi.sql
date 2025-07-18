WITH _s0 AS (
  SELECT
    AVG(c_acctbal) AS global_avg_balance
  FROM tpch.customer
  WHERE
    SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND c_acctbal > 0.0
), _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    o_custkey
  FROM tpch.orders
  GROUP BY
    o_custkey
)
SELECT
  SUBSTRING(customer.c_phone, 1, 2) AS CNTRY_CODE,
  COUNT(*) AS NUM_CUSTS,
  COALESCE(SUM(customer.c_acctbal), 0) AS TOTACCTBAL
FROM _s0 AS _s0
JOIN tpch.customer AS customer
  ON SUBSTRING(customer.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
  AND _s0.global_avg_balance < customer.c_acctbal
LEFT JOIN _s3 AS _s3
  ON _s3.o_custkey = customer.c_custkey
WHERE
  _s3.n_rows = 0 OR _s3.n_rows IS NULL
GROUP BY
  SUBSTRING(customer.c_phone, 1, 2)
ORDER BY
  cntry_code
