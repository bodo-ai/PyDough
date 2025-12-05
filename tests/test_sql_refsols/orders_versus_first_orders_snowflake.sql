WITH _s4 AS (
  SELECT
    o_custkey,
    o_orderdate,
    o_orderkey
  FROM tpch.orders
), _t1 AS (
  SELECT
    customer.c_custkey,
    customer.c_name,
    _s3.o_orderdate
  FROM tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey AND nation.n_name = 'VIETNAM'
  JOIN _s4 AS _s3
    ON _s3.o_custkey = customer.c_custkey
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY _s3.o_orderdate, _s3.o_orderkey) = 1
)
SELECT
  _t1.c_name AS customer_name,
  _s4.o_orderkey AS order_key,
  DATEDIFF(DAY, CAST(_t1.o_orderdate AS DATETIME), CAST(_s4.o_orderdate AS DATETIME)) AS days_since_first_order
FROM _s4 AS _s4
LEFT JOIN _t1 AS _t1
  ON _s4.o_custkey = _t1.c_custkey
ORDER BY
  3 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 5
