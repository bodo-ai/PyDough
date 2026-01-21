WITH _t1 AS (
  SELECT
    orders.o_custkey,
    orders.o_orderkey,
    orders.o_orderpriority,
    orders.o_totalprice,
    priority_taxes.tax_rate
  FROM tpch.orders AS orders
  JOIN (VALUES
    ('1-URGENT', 0.05),
    ('2-HIGH', 0.04),
    ('3-MEDIUM', 0.03),
    ('4-NOT SPECIFIED', 0.02)) AS priority_taxes(priority_lvl, tax_rate)
    ON orders.o_orderpriority = priority_taxes.priority_lvl
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY o_orderkey ORDER BY orders.o_totalprice + orders.o_totalprice * priority_taxes.tax_rate) = 1
)
SELECT
  customer.c_name AS name,
  _t1.o_orderkey AS order_key,
  _t1.o_orderpriority AS order_priority,
  _t1.o_totalprice + _t1.o_totalprice * _t1.tax_rate AS cheapest_order_price
FROM tpch.customer AS customer
JOIN _t1 AS _t1
  ON _t1.o_custkey = customer.c_custkey
ORDER BY
  4 NULLS FIRST
LIMIT 5
