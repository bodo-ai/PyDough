WITH _s4 AS (
  SELECT
    o_custkey,
    o_orderdate,
    o_orderkey
  FROM tpch.ORDERS
), _t AS (
  SELECT
    CUSTOMER.c_custkey,
    CUSTOMER.c_name,
    _s3.o_orderdate,
    ROW_NUMBER() OVER (PARTITION BY _s3.o_custkey ORDER BY CASE WHEN _s3.o_orderdate IS NULL THEN 1 ELSE 0 END, _s3.o_orderdate, CASE WHEN _s3.o_orderkey IS NULL THEN 1 ELSE 0 END, _s3.o_orderkey) AS _w
  FROM tpch.CUSTOMER AS CUSTOMER
  JOIN tpch.NATION AS NATION
    ON CUSTOMER.c_nationkey = NATION.n_nationkey AND NATION.n_name = 'VIETNAM'
  JOIN _s4 AS _s3
    ON CUSTOMER.c_custkey = _s3.o_custkey
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
  _s5.c_name COLLATE utf8mb4_bin AS customer_name,
  _s4.o_orderkey AS order_key,
  DATEDIFF(_s4.o_orderdate, _s5.o_orderdate) AS days_since_first_order
FROM _s4 AS _s4
LEFT JOIN _s5 AS _s5
  ON _s4.o_custkey = _s5.c_custkey
ORDER BY
  3 DESC,
  1
LIMIT 5
