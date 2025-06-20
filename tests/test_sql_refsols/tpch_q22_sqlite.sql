WITH _s0 AS (
  SELECT
    AVG(c_acctbal) AS global_avg_balance
  FROM tpch.customer
  WHERE
    SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND c_acctbal > 0.0
), _s3 AS (
  SELECT
    COUNT(*) AS agg_0,
    o_custkey AS customer_key
  FROM tpch.orders
  GROUP BY
    o_custkey
), _t1 AS (
  SELECT
    COUNT(*) AS agg_1,
    SUM(customer.c_acctbal) AS agg_2,
    SUBSTRING(customer.c_phone, 1, 2) AS cntry_code
  FROM _s0 AS _s0
  CROSS JOIN tpch.customer AS customer
  LEFT JOIN _s3 AS _s3
    ON _s3.customer_key = customer.c_custkey
  WHERE
    SUBSTRING(customer.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND _s0.global_avg_balance < customer.c_acctbal
    AND (
      _s3.agg_0 = 0 OR _s3.agg_0 IS NULL
    )
  GROUP BY
    SUBSTRING(customer.c_phone, 1, 2)
)
SELECT
  cntry_code AS CNTRY_CODE,
  agg_1 AS NUM_CUSTS,
  COALESCE(agg_2, 0) AS TOTACCTBAL
FROM _t1
ORDER BY
  cntry_code
