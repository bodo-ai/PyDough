WITH _t3 AS (
  SELECT
    o_custkey,
    o_orderdate,
    o_orderkey,
    o_orderstatus
  FROM tpch.orders
  WHERE
    o_orderstatus = 'P'
), _s3 AS (
  SELECT
    _t3.o_orderkey,
    COUNT(*) AS n_rows
  FROM _t3 AS _t3
  JOIN _t3 AS _t4
    ON _t3.o_custkey = _t4.o_custkey
    AND _t3.o_orderdate = _t4.o_orderdate
    AND _t3.o_orderkey < _t4.o_orderkey
  GROUP BY
    1
)
SELECT
  orders.o_orderkey AS original_order_key,
  COALESCE(_s3.n_rows, 0) AS n_other_orders
FROM tpch.orders AS orders
LEFT JOIN _s3 AS _s3
  ON _s3.o_orderkey = orders.o_orderkey
WHERE
  orders.o_orderstatus = 'P'
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 5
