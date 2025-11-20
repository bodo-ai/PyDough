WITH _t3 AS (
  SELECT
    o_custkey,
    o_orderdate,
    o_totalprice
  FROM tpch.orders
  WHERE
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1993
), _t1 AS (
  SELECT
    o_custkey,
    o_orderdate,
    COUNT(*) AS n_rows,
    SUM(o_totalprice) AS sum_ototalprice
  FROM _t3
  GROUP BY
    1,
    2
)
SELECT
  COUNT(*) AS n
FROM _t1 AS _t1
JOIN _t3 AS _t4
  ON _t1.o_custkey = _t4.o_custkey
  AND _t1.o_orderdate = _t4.o_orderdate
  AND _t4.o_totalprice >= (
    0.5 * COALESCE(_t1.sum_ototalprice, 0)
  )
WHERE
  _t1.n_rows > 1
