WITH _s6 AS (
  SELECT
    SUM(_s2.l_extendedprice * (
      1 - _s2.l_discount
    )) AS agg_0,
    _s1.o_custkey AS customer_key
  FROM tpch.orders AS _s1
  JOIN tpch.lineitem AS _s2
    ON _s1.o_orderkey = _s2.l_orderkey AND _s2.l_returnflag = 'R'
  WHERE
    CASE
      WHEN CAST(STRFTIME('%m', _s1.o_orderdate) AS INTEGER) <= 3
      AND CAST(STRFTIME('%m', _s1.o_orderdate) AS INTEGER) >= 1
      THEN 1
      WHEN CAST(STRFTIME('%m', _s1.o_orderdate) AS INTEGER) <= 6
      AND CAST(STRFTIME('%m', _s1.o_orderdate) AS INTEGER) >= 4
      THEN 2
      WHEN CAST(STRFTIME('%m', _s1.o_orderdate) AS INTEGER) <= 9
      AND CAST(STRFTIME('%m', _s1.o_orderdate) AS INTEGER) >= 7
      THEN 3
      WHEN CAST(STRFTIME('%m', _s1.o_orderdate) AS INTEGER) <= 12
      AND CAST(STRFTIME('%m', _s1.o_orderdate) AS INTEGER) >= 10
      THEN 4
    END = 4
    AND CAST(STRFTIME('%Y', _s1.o_orderdate) AS INTEGER) = 1993
  GROUP BY
    _s1.o_custkey
)
SELECT
  _s0.c_custkey AS C_CUSTKEY,
  _s0.c_name AS C_NAME,
  COALESCE(_s6.agg_0, 0) AS REVENUE,
  _s0.c_acctbal AS C_ACCTBAL,
  _s7.n_name AS N_NAME,
  _s0.c_address AS C_ADDRESS,
  _s0.c_phone AS C_PHONE,
  _s0.c_comment AS C_COMMENT
FROM tpch.customer AS _s0
LEFT JOIN _s6 AS _s6
  ON _s0.c_custkey = _s6.customer_key
JOIN tpch.nation AS _s7
  ON _s0.c_nationkey = _s7.n_nationkey
ORDER BY
  revenue DESC,
  c_custkey
LIMIT 20
