WITH _table_alias_0 AS (
  SELECT
    AVG(customer.c_acctbal) AS global_avg_balance
  FROM tpch.customer AS customer
  WHERE
    SUBSTRING(customer.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND customer.c_acctbal > 0.0
), _table_alias_1 AS (
  SELECT
    customer.c_acctbal AS acctbal,
    customer.c_custkey AS key,
    customer.c_phone AS phone
  FROM tpch.customer AS customer
  WHERE
    SUBSTRING(customer.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
), _table_alias_2 AS (
  SELECT
    _table_alias_1.acctbal AS acctbal,
    SUBSTRING(_table_alias_1.phone, 1, 2) AS cntry_code,
    _table_alias_1.key AS key
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.global_avg_balance < _table_alias_1.acctbal
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
    SUM(_table_alias_2.acctbal) AS agg_2,
    _table_alias_2.cntry_code AS cntry_code
  FROM _table_alias_2 AS _table_alias_2
  LEFT JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_2.key = _table_alias_3.customer_key
  WHERE
    _table_alias_3.agg_0 = 0 OR _table_alias_3.agg_0 IS NULL
  GROUP BY
    _table_alias_2.cntry_code
)
SELECT
  _t1.cntry_code AS CNTRY_CODE,
  COALESCE(_t1.agg_1, 0) AS NUM_CUSTS,
  COALESCE(_t1.agg_2, 0) AS TOTACCTBAL
FROM _t1 AS _t1
ORDER BY
  _t1.cntry_code
