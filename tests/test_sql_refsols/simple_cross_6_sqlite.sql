WITH _t1 AS (
  SELECT
    o_clerk,
    o_custkey,
    o_orderdate,
    o_orderkey
  FROM tpch.orders
  WHERE
    CAST(SUBSTRING(o_clerk, 7) AS INTEGER) >= 900
)
SELECT
  COUNT(*) AS n_pairs
FROM _t1 AS _t1
JOIN _t1 AS _t2
  ON _t1.o_custkey = _t2.o_custkey
  AND _t1.o_orderdate = _t2.o_orderdate
  AND _t1.o_orderkey < _t2.o_orderkey
