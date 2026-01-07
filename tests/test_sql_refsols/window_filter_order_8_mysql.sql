WITH _s3 AS (
  SELECT
    o_custkey
  FROM tpch.ORDERS
  WHERE
    EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) = 1
    AND EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) = 1998
), _t2 AS (
  SELECT
    ANY_VALUE(CUSTOMER.c_acctbal) AS anything_c_acctbal,
    COUNT(_s3.o_custkey) AS count_o_custkey
  FROM tpch.CUSTOMER AS CUSTOMER
  JOIN tpch.NATION AS NATION
    ON CUSTOMER.c_nationkey = NATION.n_nationkey AND NATION.n_name = 'FRANCE'
  LEFT JOIN _s3 AS _s3
    ON CUSTOMER.c_custkey = _s3.o_custkey
  GROUP BY
    CUSTOMER.c_custkey
), _t AS (
  SELECT
    anything_c_acctbal,
    count_o_custkey,
    SUM(COALESCE(count_o_custkey, 0)) OVER () AS _w
  FROM _t2
)
SELECT
  COUNT(*) AS n
FROM _t
WHERE
  NULLIF(count_o_custkey, 0) IS NULL AND _w > anything_c_acctbal
