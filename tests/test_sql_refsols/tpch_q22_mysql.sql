WITH _s0 AS (
  SELECT
    AVG(c_acctbal) AS avg_c_acctbal
  FROM tpch.CUSTOMER
  WHERE
    c_acctbal > 0.0
    AND SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
)
SELECT
  SUBSTRING(CUSTOMER.c_phone, 1, 2) COLLATE utf8mb4_bin AS CNTRY_CODE,
  COUNT(*) AS NUM_CUSTS,
  COALESCE(SUM(CUSTOMER.c_acctbal), 0) AS TOTACCTBAL
FROM _s0 AS _s0
JOIN tpch.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_acctbal > _s0.avg_c_acctbal
  AND SUBSTRING(CUSTOMER.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
WHERE
  NOT EXISTS(
    SELECT
      1 AS `1`
    FROM tpch.ORDERS
    WHERE
      CUSTOMER.c_custkey = o_custkey
  )
GROUP BY
  1
ORDER BY
  1
