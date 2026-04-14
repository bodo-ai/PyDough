WITH _s4 AS (
  SELECT
    o_custkey,
    o_orderdate,
    o_orderkey
  FROM tpch.orders
), _t AS (
  SELECT
    customer.c_custkey,
    customer.c_name,
    _s3.o_orderdate,
    ROW_NUMBER() OVER (PARTITION BY _s3.o_custkey ORDER BY _s3.o_orderdate, _s3.o_orderkey) AS _w
  FROM tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey AND nation.n_name = 'VIETNAM'
  JOIN _s4 AS _s3
    ON _s3.o_custkey = customer.c_custkey
), _s5 AS (
  SELECT
    c_custkey,
    c_name,
    o_orderdate
  FROM _t
  WHERE
    _w = 1
)
SELECT
  _s5.c_name AS customer_name,
  _s4.o_orderkey AS order_key,
  CAST(_s4.o_orderdate AS DATE) - CAST(_s5.o_orderdate AS DATE) AS days_since_first_order
FROM _s4 AS _s4
LEFT JOIN _s5 AS _s5
  ON _s4.o_custkey = _s5.c_custkey
ORDER BY
  3 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 5
