WITH _t3 AS (
  SELECT
    AVG(customer.c_acctbal) OVER (PARTITION BY nation.n_regionkey) AS avg_balance,
    customer.c_acctbal,
    nation.n_regionkey
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
), _s3 AS (
  SELECT
    COALESCE(COUNT(*), 0) AS n_cust,
    n_regionkey
  FROM _t3
  WHERE
    ABS(avg_balance - c_acctbal) <= avg_balance * 0.1
  GROUP BY
    n_regionkey
)
SELECT
  region.r_name AS name,
  _s3.n_cust
FROM tpch.region AS region
JOIN _s3 AS _s3
  ON _s3.n_regionkey = region.r_regionkey
ORDER BY
  region.r_name
