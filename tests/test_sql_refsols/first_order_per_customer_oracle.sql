WITH _T AS (
  SELECT
    o_custkey AS O_CUSTKEY,
    o_orderdate AS O_ORDERDATE,
    o_totalprice AS O_TOTALPRICE,
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_orderdate, o_orderkey) AS _W
  FROM TPCH.ORDERS
)
SELECT
  CUSTOMER.c_name AS name,
  _T.O_ORDERDATE AS first_order_date,
  _T.O_TOTALPRICE AS first_order_price
FROM TPCH.CUSTOMER CUSTOMER
JOIN _T _T
  ON CUSTOMER.c_custkey = _T.O_CUSTKEY AND _T._W = 1
WHERE
  CUSTOMER.c_acctbal >= 9000.0
ORDER BY
  3 DESC NULLS LAST
FETCH FIRST 5 ROWS ONLY
