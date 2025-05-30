WITH _t AS (
  SELECT
    o_custkey AS customer_key,
    o_orderdate AS order_date,
    o_totalprice AS total_price,
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_orderdate, o_orderkey) AS _w
  FROM tpch.orders
)
SELECT
  customer.c_name AS name,
  _t.order_date AS first_order_date,
  _t.total_price AS first_order_price
FROM tpch.customer AS customer
JOIN _t AS _t
  ON _t._w = 1 AND _t.customer_key = customer.c_custkey
WHERE
  customer.c_acctbal >= 9000.0
ORDER BY
  first_order_price DESC
LIMIT 5
