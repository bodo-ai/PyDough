WITH _s4 AS (
  SELECT
    o_custkey AS customer_key,
    o_orderkey AS key,
    o_orderdate AS order_date
  FROM tpch.orders
), _t2 AS (
  SELECT
    customer.c_custkey AS key,
    customer.c_name AS customer_name,
    _s3.order_date
  FROM tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey AND nation.n_name = 'VIETNAM'
  JOIN _s4 AS _s3
    ON _s3.customer_key = customer.c_custkey
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY customer.c_custkey ORDER BY _s3.order_date NULLS LAST, _s3.key NULLS LAST) = 1
)
SELECT
  _t2.customer_name,
  _s4.key AS order_key,
  DATEDIFF(_s4.order_date, _t2.order_date, DAY) AS days_since_first_order
FROM _s4 AS _s4
LEFT JOIN _t2 AS _t2
  ON _s4.customer_key = _t2.key
ORDER BY
  days_since_first_order DESC,
  customer_name
LIMIT 5
