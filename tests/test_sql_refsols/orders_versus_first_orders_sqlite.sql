WITH _s4 AS (
  SELECT
    o_custkey AS customer_key,
    o_orderkey AS key,
    o_orderdate AS order_date
  FROM tpch.orders
), _t AS (
  SELECT
    customer.c_custkey AS key,
    customer.c_name AS customer_name,
    _s3.order_date,
    ROW_NUMBER() OVER (PARTITION BY customer.c_custkey ORDER BY _s3.order_date, _s3.key) AS _w
  FROM tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey AND nation.n_name = 'VIETNAM'
  JOIN _s4 AS _s3
    ON _s3.customer_key = customer.c_custkey
), _s5 AS (
  SELECT
    customer_name,
    key,
    order_date
  FROM _t
  WHERE
    _w = 1
)
SELECT
  _s5.customer_name,
  _s4.key AS order_key,
  CAST((
    JULIANDAY(DATE(_s4.order_date, 'start of day')) - JULIANDAY(DATE(_s5.order_date, 'start of day'))
  ) AS INTEGER) AS days_since_first_order
FROM _s4 AS _s4
LEFT JOIN _s5 AS _s5
  ON _s4.customer_key = _s5.key
ORDER BY
  days_since_first_order DESC,
  customer_name
LIMIT 5
