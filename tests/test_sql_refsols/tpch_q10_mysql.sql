WITH _s3 AS (
  SELECT
    SUM(LINEITEM.l_extendedprice * (
      1 - LINEITEM.l_discount
    )) AS agg_0,
    ORDERS.o_custkey
  FROM tpch.ORDERS AS ORDERS
  JOIN tpch.LINEITEM AS LINEITEM
    ON LINEITEM.l_orderkey = ORDERS.o_orderkey AND LINEITEM.l_returnflag = 'R'
  WHERE
    EXTRACT(MONTH FROM CAST(ORDERS.o_orderdate AS DATETIME)) IN (10, 11, 12)
    AND EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) = 1993
  GROUP BY
    2
)
SELECT
  CUSTOMER.c_custkey AS C_CUSTKEY,
  CUSTOMER.c_name AS C_NAME,
  COALESCE(_s3.agg_0, 0) AS REVENUE,
  CUSTOMER.c_acctbal AS C_ACCTBAL,
  NATION.n_name AS N_NAME,
  CUSTOMER.c_address AS C_ADDRESS,
  CUSTOMER.c_phone AS C_PHONE,
  CUSTOMER.c_comment AS C_COMMENT
FROM tpch.CUSTOMER AS CUSTOMER
LEFT JOIN _s3 AS _s3
  ON CUSTOMER.c_custkey = _s3.o_custkey
JOIN tpch.NATION AS NATION
  ON CUSTOMER.c_nationkey = NATION.n_nationkey
ORDER BY
  3 DESC,
  1
LIMIT 20
