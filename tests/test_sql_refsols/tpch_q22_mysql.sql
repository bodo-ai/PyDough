WITH _s0 AS (
  SELECT
    AVG(c_acctbal) AS avg_cacctbal
  FROM tpch.CUSTOMER
  WHERE
    c_acctbal > 0.0
    AND SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
), _s3 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.ORDERS
  GROUP BY
    1
)
SELECT
  SUBSTRING(CUSTOMER.c_phone, 1, 2) COLLATE utf8mb4_bin AS CNTRY_CODE,
  COUNT(*) AS NUM_CUSTS,
  COALESCE(SUM(CUSTOMER.c_acctbal), 0) AS TOTACCTBAL
FROM _s0 AS _s0
JOIN tpch.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_acctbal > _s0.avg_cacctbal
  AND SUBSTRING(CUSTOMER.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
LEFT JOIN _s3 AS _s3
  ON CUSTOMER.c_custkey = _s3.o_custkey
WHERE
  _s3.n_rows = 0 OR _s3.n_rows IS NULL
GROUP BY
  1
ORDER BY
  1
