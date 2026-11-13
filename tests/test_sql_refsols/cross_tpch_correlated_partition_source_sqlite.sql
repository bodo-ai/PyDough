WITH _t1 AS (
  SELECT
    c_custkey
  FROM tpch.customer
  WHERE
    c_custkey <= 2
), _s3 AS (
  SELECT DISTINCT
    _t3.c_custkey,
    orders.o_custkey
  FROM _t1 AS _t3
  JOIN tpch.orders AS orders
    ON orders.o_custkey <= 2 AND orders.o_totalprice > (
      _t3.c_custkey * 100000
    )
), _s7 AS (
  SELECT
    _t5.c_custkey,
    orders.o_custkey,
    orders.o_orderkey,
    orders.o_totalprice
  FROM _t1 AS _t5
  JOIN tpch.orders AS orders
    ON orders.o_custkey <= 2 AND orders.o_totalprice > (
      _t5.c_custkey * 100000
    )
)
SELECT
  _t1.c_custkey AS k,
  _s7.o_custkey AS c,
  _s7.o_orderkey AS oid,
  SUM(_s7.o_totalprice) OVER (
    PARTITION BY _s3.o_custkey, _t1.c_custkey
    ORDER BY _s7.o_orderkey
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running
FROM _t1 AS _t1
LEFT JOIN _s3 AS _s3
  ON _s3.c_custkey = _t1.c_custkey
JOIN _s7 AS _s7
  ON _s3.o_custkey = _s7.o_custkey AND _s7.c_custkey = _t1.c_custkey
