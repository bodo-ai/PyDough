WITH _t AS (
  SELECT
    customer.c_custkey,
    ROW_NUMBER() OVER (PARTITION BY nation.n_regionkey ORDER BY customer.c_acctbal DESC) AS _w
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
)
SELECT
  c_custkey AS key
FROM _t
WHERE
  _w = 1
