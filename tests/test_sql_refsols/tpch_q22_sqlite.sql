WITH _s0 AS (
  SELECT
    AVG(c_acctbal) AS avg_c_acctbal
  FROM tpch.customer
  WHERE
    SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND c_acctbal > 0.0
), _t5 AS (
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
  SUM(customer.c_acctbal) AS TOTACCTBAL
FROM _s0 AS _s0
JOIN tpch.customer AS customer
  ON SUBSTRING(customer.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
  AND _s0.avg_c_acctbal < customer.c_acctbal
JOIN _t5 AS _t5
  ON _t5.n_rows = 0 AND _t5.o_custkey = customer.c_custkey
GROUP BY
  1
ORDER BY
  1
