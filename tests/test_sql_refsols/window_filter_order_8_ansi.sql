WITH _s3 AS (
  SELECT
    o_custkey
  FROM tpch.orders
  WHERE
    EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) = 1
    AND EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) = 1998
), _t2 AS (
  SELECT
    MAX(1) AS "_"
  FROM tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey AND nation.n_name = 'FRANCE'
  LEFT JOIN _s3 AS _s3
    ON _s3.o_custkey = customer.c_custkey
  GROUP BY
    customer.c_custkey
), _t1 AS (
  SELECT
    1 AS "_"
  FROM _t2
  QUALIFY
    NULLIF(count_o_custkey, 0) IS NULL
    AND anything_c_acctbal < SUM(COALESCE(count_o_custkey, 0)) OVER ()
)
SELECT
  COUNT(*) AS n
FROM _t1
