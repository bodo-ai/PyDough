WITH _t4 AS (
  SELECT
    o_custkey,
    o_orderdate,
    o_totalprice
  FROM tpch.orders
  WHERE
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) = 1993
), _t2 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(o_totalprice) AS sum_o_totalprice,
    o_custkey,
    o_orderdate
  FROM _t4
  GROUP BY
    o_custkey,
    o_orderdate
)
SELECT
  COUNT(*) AS n
FROM _t2 AS _t2
JOIN _t4 AS _t5
  ON _t2.o_custkey = _t5.o_custkey
  AND _t2.o_orderdate = _t5.o_orderdate
  AND _t5.o_totalprice >= (
    0.5 * COALESCE(_t2.sum_o_totalprice, 0)
  )
WHERE
  _t2.n_rows > 1
