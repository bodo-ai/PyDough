WITH "_S0" AS (
  SELECT
    AVG(c_acctbal) AS AVG_C_ACCTBAL
  FROM TPCH.CUSTOMER
  WHERE
    c_acctbal > 0.0
    AND SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
), "_u_0" AS (
  SELECT
    o_custkey AS "_u_1"
  FROM TPCH.ORDERS
  GROUP BY
    o_custkey
)
SELECT
  SUBSTR(CUSTOMER.c_phone, 1, 2) AS CNTRY_CODE,
  COUNT(*) AS NUM_CUSTS,
  COALESCE(SUM(CUSTOMER.c_acctbal), 0) AS TOTACCTBAL
FROM "_S0" "_S0"
JOIN TPCH.CUSTOMER CUSTOMER
  ON CUSTOMER.c_acctbal > "_S0".AVG_C_ACCTBAL
  AND SUBSTR(CUSTOMER.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
LEFT JOIN "_u_0" "_u_0"
  ON CUSTOMER.c_custkey = "_u_0"."_u_1"
WHERE
  "_u_0"."_u_1" IS NULL
GROUP BY
  SUBSTR(CUSTOMER.c_phone, 1, 2)
ORDER BY
  1 NULLS FIRST
