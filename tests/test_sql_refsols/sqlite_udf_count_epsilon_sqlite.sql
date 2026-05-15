WITH _t2 AS (
  SELECT
    customer.c_acctbal,
    nation.n_regionkey,
    AVG(CAST(customer.c_acctbal AS REAL)) OVER (PARTITION BY nation.n_regionkey) AS avg_balance
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
), _s3 AS (
  SELECT
    n_regionkey,
    COUNT(*) AS n_rows
  FROM _t2
  WHERE
    ABS(avg_balance - c_acctbal) <= avg_balance * 0.1
  GROUP BY
    1
)
SELECT
  region.r_name AS name,
  _s3.n_rows AS n_cust
FROM tpch.region AS region
JOIN _s3 AS _s3
  ON _s3.n_regionkey = region.r_regionkey
ORDER BY
  1
