WITH _s0 AS (
  SELECT
    AVG(c_acctbal) AS avg_c_acctbal
  FROM tpch.CUSTOMER
  WHERE
    c_acctbal > 0.0
    AND SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
), _s3 AS (
  SELECT
    o_custkey
  FROM tpch.ORDERS
), _t2 AS (
  SELECT
    ANY_VALUE(CUSTOMER.c_acctbal) AS anything_c_acctbal,
    ANY_VALUE(CUSTOMER.c_phone) AS anything_c_phone,
    COUNT(*) AS n_rows
  FROM _s0 AS _s0
  JOIN tpch.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_acctbal > _s0.avg_c_acctbal
    AND SUBSTRING(CUSTOMER.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
  LEFT JOIN _s3 AS _s3
    ON CUSTOMER.c_custkey = _s3.o_custkey
  GROUP BY
    _s3.o_custkey
)
SELECT
  SUBSTRING(anything_c_phone, 1, 2) COLLATE utf8mb4_bin AS CNTRY_CODE,
  COUNT(*) AS NUM_CUSTS,
  COALESCE(SUM(anything_c_acctbal), 0) AS TOTACCTBAL
FROM _t2
WHERE
  n_rows = 0
GROUP BY
  1
ORDER BY
  1
