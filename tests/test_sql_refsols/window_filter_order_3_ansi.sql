WITH _s3 AS (
  SELECT
    o_custkey
  FROM tpch.orders
  WHERE
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) = 1992
), _t2 AS (
  SELECT
    MAX(1) AS "_"
  FROM tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey AND nation.n_name = 'GERMANY'
  LEFT JOIN _s3 AS _s3
    ON _s3.o_custkey = customer.c_custkey
  GROUP BY
    customer.c_custkey
), _t1 AS (
  SELECT
    1 AS "_"
  FROM _t2
  QUALIFY
    COALESCE(count_o_custkey, 0) < AVG(COALESCE(count_o_custkey, 0)) OVER ()
    AND NULLIF(count_o_custkey, 0) <> 0
)
SELECT
  COUNT(*) AS n
FROM _t1
