WITH _s0 AS (
  SELECT
    c_custkey AS key,
    c_name AS name
  FROM tpch.customer
  ORDER BY
    name
  LIMIT 5
), _t2 AS (
  SELECT
    o_custkey AS customer_key,
    o_orderdate AS order_date
  FROM tpch.orders
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_totalprice DESC NULLS FIRST) = 1
)
SELECT
  _s0.name
FROM _s0 AS _s0
LEFT JOIN _t2 AS _t2
  ON _s0.key = _t2.customer_key
ORDER BY
  _t2.order_date NULLS LAST
