WITH _s0 AS (
  SELECT
    AVG(c_acctbal) AS avg_c_acctbal
  FROM tpch.CUSTOMER
  WHERE
    c_acctbal > 0.0
    AND SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
), _t5 AS (
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
  SUM(CUSTOMER.c_acctbal) AS TOTACCTBAL
FROM _s0 AS _s0
JOIN tpch.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_acctbal > _s0.avg_c_acctbal
  AND SUBSTRING(CUSTOMER.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
JOIN _t5 AS _t5
  ON CUSTOMER.c_custkey = _t5.o_custkey AND _t5.n_rows = 0
GROUP BY
  1
ORDER BY
  1
