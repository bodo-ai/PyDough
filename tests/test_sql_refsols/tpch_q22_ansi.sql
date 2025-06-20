WITH _s2 AS (
  SELECT
    AVG(c_acctbal) AS global_avg_balance
  FROM tpch.customer
  WHERE
    SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND c_acctbal > 0.0
), _s6 AS (
  SELECT
    COUNT(*) AS agg_0,
    o_custkey AS customer_key
  FROM tpch.orders
  GROUP BY
    o_custkey
), _t1 AS (
  SELECT
    COUNT(*) AS agg_1,
    SUM(_s1.c_acctbal) AS agg_2,
    SUBSTRING(_s1.c_phone, 1, 2) AS cntry_code
  FROM _s2 AS _s2
  CROSS JOIN tpch.customer AS _s1
  LEFT JOIN _s6 AS _s6
    ON _s1.c_custkey = _s6.customer_key
  WHERE
    SUBSTRING(_s1.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND _s1.c_acctbal > _s2.global_avg_balance
    AND (
      _s6.agg_0 = 0 OR _s6.agg_0 IS NULL
    )
  GROUP BY
    SUBSTRING(_s1.c_phone, 1, 2)
)
SELECT
  cntry_code AS CNTRY_CODE,
  agg_1 AS NUM_CUSTS,
  COALESCE(agg_2, 0) AS TOTACCTBAL
FROM _t1
ORDER BY
  cntry_code
