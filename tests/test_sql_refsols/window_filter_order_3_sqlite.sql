WITH _s3 AS (
  SELECT
    o_custkey
  FROM tpch.orders
  WHERE
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1992
), _t2 AS (
  SELECT
    COUNT(_s3.o_custkey) AS count_o_custkey
  FROM tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey AND nation.n_name = 'GERMANY'
  LEFT JOIN _s3 AS _s3
    ON _s3.o_custkey = customer.c_custkey
  GROUP BY
    customer.c_custkey
), _t AS (
  SELECT
    count_o_custkey,
    AVG(CAST(COALESCE(count_o_custkey, 0) AS REAL)) OVER () AS _w
  FROM _t2
)
SELECT
  COUNT(*) AS n
FROM _t
WHERE
  NULLIF(count_o_custkey, 0) <> 0 AND _w > COALESCE(count_o_custkey, 0)
