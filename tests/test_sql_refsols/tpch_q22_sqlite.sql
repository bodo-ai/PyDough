WITH _s0 AS (
  SELECT
    AVG(c_acctbal) AS avg_cacctbal
  FROM tpch.customer
  WHERE
    SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND c_acctbal > 0.0
), _s3 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    1
)
SELECT
  SUBSTRING(customer.c_phone, 1, 2) AS CNTRY_CODE,
  COUNT(*) AS NUM_CUSTS,
  COALESCE(SUM(customer.c_acctbal), 0) AS TOTACCTBAL
FROM _s0 AS _s0
JOIN tpch.customer AS customer
  ON SUBSTRING(customer.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
  AND _s0.avg_cacctbal < customer.c_acctbal
LEFT JOIN _s3 AS _s3
  ON _s3.o_custkey = customer.c_custkey
WHERE
  _s3.n_rows = 0 OR _s3.n_rows IS NULL
GROUP BY
  1
ORDER BY
  1
