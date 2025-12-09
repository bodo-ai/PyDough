WITH _t AS (
  SELECT
    o_custkey,
    o_orderdate,
    o_totalprice,
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_orderdate, o_orderkey) AS _w
  FROM tpch.orders
)
SELECT
  customer.c_name AS name,
  _t.o_orderdate AS first_order_date,
  _t.o_totalprice AS first_order_price
FROM tpch.customer AS customer
JOIN _t AS _t
  ON _t._w = 1 AND _t.o_custkey = customer.c_custkey
WHERE
  customer.c_acctbal >= 9000.0
ORDER BY
  3 DESC NULLS LAST
LIMIT 5
