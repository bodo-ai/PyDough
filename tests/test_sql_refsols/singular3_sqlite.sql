WITH _s0 AS (
  SELECT
    c_custkey AS key,
    c_name AS name
  FROM tpch.customer
  ORDER BY
    name
  LIMIT 5
), _t AS (
  SELECT
    o_custkey AS customer_key,
    o_orderdate AS order_date,
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_totalprice DESC) AS _w
  FROM tpch.orders
), _s1 AS (
  SELECT
    customer_key,
    order_date
  FROM _t
  WHERE
    _w = 1
)
SELECT
  _s0.name
FROM _s0 AS _s0
LEFT JOIN _s1 AS _s1
  ON _s0.key = _s1.customer_key
ORDER BY
  _s1.order_date
