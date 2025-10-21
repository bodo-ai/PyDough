WITH _s0 AS (
  SELECT
    AVG(CAST(c_acctbal AS DECIMAL)) AS avg_c_acctbal
  FROM tpch.customer
  WHERE
    SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND c_acctbal > 0.0
), _s3 AS (
  SELECT
    o_custkey
  FROM tpch.orders
), _t2 AS (
  SELECT
    _s3.o_custkey,
    MAX(customer.c_acctbal) AS anything_c_acctbal,
    MAX(customer.c_phone) AS anything_c_phone,
    COUNT(*) AS n_rows
  FROM _s0 AS _s0
  JOIN tpch.customer AS customer
    ON SUBSTRING(customer.c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND _s0.avg_c_acctbal < customer.c_acctbal
  LEFT JOIN _s3 AS _s3
    ON _s3.o_custkey = customer.c_custkey
  GROUP BY
    1
)
SELECT
  SUBSTRING(anything_c_phone FROM 1 FOR 2) AS CNTRY_CODE,
  COUNT(*) AS NUM_CUSTS,
  COALESCE(SUM(anything_c_acctbal), 0) AS TOTACCTBAL
FROM _t2
WHERE
  (
    n_rows * CASE WHEN NOT o_custkey IS NULL THEN 1 ELSE 0 END
  ) = 0
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
