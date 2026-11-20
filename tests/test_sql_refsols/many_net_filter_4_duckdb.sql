WITH _s1 AS (
  SELECT
    n_nationkey,
    n_regionkey
  FROM main.nation
)
SELECT
  COUNT(*) AS n
FROM main.supplier AS supplier
JOIN _s1 AS _s1
  ON _s1.n_nationkey = supplier.s_nationkey
JOIN _s1 AS _s3
  ON _s1.n_regionkey = _s3.n_regionkey
JOIN main.customer AS customer
  ON _s3.n_nationkey = customer.c_nationkey
  AND customer.c_custkey = supplier.s_suppkey
WHERE
  supplier.s_nationkey = 4
