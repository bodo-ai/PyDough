WITH _t0 AS (
  SELECT
    COUNT(*) AS order_count,
    _s0.o_orderpriority AS o_orderpriority
  FROM tpch.orders AS _s0
  JOIN tpch.lineitem AS _s1
    ON _s0.o_orderkey = _s1.l_orderkey AND _s1.l_commitdate < _s1.l_receiptdate
  WHERE
    EXTRACT(QUARTER FROM _s0.o_orderdate) = 3
    AND EXTRACT(YEAR FROM _s0.o_orderdate) = 1993
  GROUP BY
    _s0.o_orderpriority
)
SELECT
  _t0.o_orderpriority AS O_ORDERPRIORITY,
  _t0.order_count AS ORDER_COUNT
FROM _t0 AS _t0
ORDER BY
  o_orderpriority
