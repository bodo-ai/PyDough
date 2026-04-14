WITH _t AS (
  SELECT
    o_custkey,
    o_orderdate,
    o_totalprice,
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY CASE WHEN o_orderdate IS NULL THEN 1 ELSE 0 END, o_orderdate, CASE WHEN o_orderkey IS NULL THEN 1 ELSE 0 END, o_orderkey) AS _w
  FROM tpch.ORDERS
)
SELECT
  CUSTOMER.c_name AS name,
  _t.o_orderdate AS first_order_date,
  _t.o_totalprice AS first_order_price
FROM tpch.CUSTOMER AS CUSTOMER
JOIN _t AS _t
  ON CUSTOMER.c_custkey = _t.o_custkey AND _t._w = 1
WHERE
  CUSTOMER.c_acctbal >= 9000.0
ORDER BY
  3 DESC
LIMIT 5
