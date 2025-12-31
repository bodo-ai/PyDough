WITH _t1 AS (
  SELECT
    c_acctbal,
    c_custkey,
    c_mktsegment,
    c_nationkey
  FROM tpch.CUSTOMER
  WHERE
    c_acctbal > 9990
)
SELECT
  COUNT(*) AS n_pairs
FROM _t1 AS _t1
JOIN _t1 AS _t2
  ON _t1.c_custkey < _t2.c_custkey
  AND _t1.c_mktsegment = _t2.c_mktsegment
  AND _t1.c_nationkey = _t2.c_nationkey
