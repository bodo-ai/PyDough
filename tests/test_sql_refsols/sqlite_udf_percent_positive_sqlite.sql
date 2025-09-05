WITH _s0 AS (
  SELECT
    n_nationkey,
    n_regionkey
  FROM tpch.nation
), _s3 AS (
  SELECT
    CAST((
      100.0 * SUM(CASE WHEN customer.c_acctbal > 0 THEN 1 END)
    ) AS REAL) / COUNT(*) AS percentage_positive_c_acctbal,
    _s0.n_regionkey
  FROM _s0 AS _s0
  JOIN tpch.customer AS customer
    ON _s0.n_nationkey = customer.c_nationkey
  GROUP BY
    2
), _s7 AS (
  SELECT
    CAST((
      100.0 * SUM(CASE WHEN supplier.s_acctbal > 0 THEN 1 END)
    ) AS REAL) / COUNT(*) AS percentage_positive_s_acctbal,
    _s4.n_regionkey
  FROM _s0 AS _s4
  JOIN tpch.supplier AS supplier
    ON _s4.n_nationkey = supplier.s_nationkey
  GROUP BY
    2
)
SELECT
  region.r_name AS name,
  ROUND(_s3.percentage_positive_c_acctbal, 2) AS pct_cust_positive,
  ROUND(_s7.percentage_positive_s_acctbal, 2) AS pct_supp_positive
FROM tpch.region AS region
JOIN _s3 AS _s3
  ON _s3.n_regionkey = region.r_regionkey
JOIN _s7 AS _s7
  ON _s7.n_regionkey = region.r_regionkey
ORDER BY
  1
