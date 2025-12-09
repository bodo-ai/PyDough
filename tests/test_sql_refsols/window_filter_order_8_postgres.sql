WITH _s3 AS (
  SELECT
    o_custkey
  FROM tpch.orders
  WHERE
    EXTRACT(MONTH FROM CAST(o_orderdate AS TIMESTAMP)) = 1
    AND EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) = 1998
), _t2 AS (
  SELECT
    MAX(customer.c_acctbal) AS anything_c_acctbal,
    COUNT(_s3.o_custkey) AS count_o_custkey
  FROM tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey AND nation.n_name = 'FRANCE'
  LEFT JOIN _s3 AS _s3
    ON _s3.o_custkey = customer.c_custkey
  GROUP BY
    customer.c_custkey
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
