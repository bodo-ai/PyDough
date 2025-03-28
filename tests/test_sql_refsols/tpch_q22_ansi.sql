WITH _table_alias_0 AS (
  SELECT
    AVG(customer.c_acctbal) AS global_avg_balance
  FROM tpch.customer AS customer
  WHERE
    SUBSTRING(customer.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND customer.c_acctbal > 0.0
), _table_alias_3 AS (
  SELECT
    COUNT() AS agg_0,
    orders.o_custkey AS customer_key
  FROM tpch.orders AS orders
  GROUP BY
    orders.o_custkey
), _t1 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(customer.c_acctbal) AS agg_2,
    SUBSTRING(customer.c_phone, 1, 2) AS cntry_code
  FROM _table_alias_0 AS _table_alias_0
  JOIN tpch.customer AS customer
    ON SUBSTRING(customer.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND _table_alias_0.global_avg_balance < customer.c_acctbal
  LEFT JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_3.customer_key = customer.c_custkey
  WHERE
    _table_alias_3.agg_0 = 0 OR _table_alias_3.agg_0 IS NULL
  GROUP BY
    SUBSTRING(customer.c_phone, 1, 2)
)
SELECT
  _t1.cntry_code AS CNTRY_CODE,
  COALESCE(_t1.agg_1, 0) AS NUM_CUSTS,
  COALESCE(_t1.agg_2, 0) AS TOTACCTBAL
FROM _t1 AS _t1
ORDER BY
  _t1.cntry_code
