WITH _s3 AS (
  SELECT
    o_custkey
  FROM tpch.ORDERS
  WHERE
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) = 1992
), _t2 AS (
  SELECT
    COUNT(_s3.o_custkey) AS count_o_custkey
  FROM tpch.CUSTOMER AS CUSTOMER
  JOIN tpch.NATION AS NATION
    ON CUSTOMER.c_nationkey = NATION.n_nationkey AND NATION.n_name = 'GERMANY'
  LEFT JOIN _s3 AS _s3
    ON CUSTOMER.c_custkey = _s3.o_custkey
  GROUP BY
    CUSTOMER.c_custkey
), _t AS (
  SELECT
    count_o_custkey,
    AVG(CAST(COALESCE(count_o_custkey, 0) AS DOUBLE)) OVER () AS _w
  FROM _t2
)
SELECT
  COUNT(*) AS n
FROM _t
WHERE
  NULLIF(count_o_custkey, 0) <> 0 AND _w > COALESCE(count_o_custkey, 0)
