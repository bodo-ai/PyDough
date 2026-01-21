WITH _t AS (
  SELECT
    orders.o_custkey,
    orders.o_orderkey,
    orders.o_orderpriority,
    orders.o_totalprice,
    priority_taxes.tax_rate,
    ROW_NUMBER() OVER (PARTITION BY orders.o_orderkey ORDER BY orders.o_totalprice + orders.o_totalprice * priority_taxes.tax_rate) AS _w
  FROM tpch.orders AS orders
  JOIN (VALUES
    ('1-URGENT', 0.05),
    ('2-HIGH', 0.04),
    ('3-MEDIUM', 0.03),
    ('4-NOT SPECIFIED', 0.02)) AS priority_taxes(priority_lvl, tax_rate)
    ON orders.o_orderpriority = priority_taxes.priority_lvl
)
SELECT
  customer.c_name AS name,
  _t.o_orderkey AS order_key,
  _t.o_orderpriority AS order_priority,
  _t.o_totalprice + _t.o_totalprice * _t.tax_rate AS cheapest_order_price
FROM tpch.customer AS customer
JOIN _t AS _t
  ON _t._w = 1 AND _t.o_custkey = customer.c_custkey
ORDER BY
  4 NULLS FIRST
LIMIT 5
