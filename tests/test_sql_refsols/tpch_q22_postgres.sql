WITH _s0 AS (
  SELECT
    AVG(CAST(c_acctbal AS DECIMAL)) AS avg_c_acctbal
  FROM tpch.customer
  WHERE
    SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND c_acctbal > 0.0
), _u_0 AS (
  SELECT
    o_custkey AS _u_1
  FROM tpch.orders
  GROUP BY
    1
)
SELECT
  SUBSTRING(customer.c_phone FROM 1 FOR 2) AS CNTRY_CODE,
  COUNT(*) AS NUM_CUSTS,
  COALESCE(SUM(customer.c_acctbal), 0) AS TOTACCTBAL
FROM _s0 AS _s0
JOIN tpch.customer AS customer
  ON SUBSTRING(customer.c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
  AND _s0.avg_c_acctbal < customer.c_acctbal
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = customer.c_custkey
WHERE
  _u_0._u_1 IS NULL
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
