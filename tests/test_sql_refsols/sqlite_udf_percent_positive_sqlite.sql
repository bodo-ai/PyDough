WITH _s0 AS (
  SELECT
    n_nationkey,
    n_regionkey
  FROM tpch.nation
), _s3 AS (
  SELECT
    ROUND(
      CAST((
        100.0 * SUM(CASE WHEN customer.c_acctbal > 0 THEN 1 END)
      ) AS REAL) / COUNT(*),
      2
    ) AS pct_cust_positive,
    _s0.n_regionkey
  FROM _s0 AS _s0
  JOIN tpch.customer AS customer
    ON _s0.n_nationkey = customer.c_nationkey
  GROUP BY
    _s0.n_regionkey
), _s7 AS (
  SELECT
    ROUND(
      CAST((
        100.0 * SUM(CASE WHEN supplier.s_acctbal > 0 THEN 1 END)
      ) AS REAL) / COUNT(*),
      2
    ) AS pct_supp_positive,
    _s4.n_regionkey
  FROM _s0 AS _s4
  JOIN tpch.supplier AS supplier
    ON _s4.n_nationkey = supplier.s_nationkey
  GROUP BY
    _s4.n_regionkey
)
SELECT
  region.r_name AS name,
  _s3.pct_cust_positive,
  _s7.pct_supp_positive
FROM tpch.region AS region
JOIN _s3 AS _s3
  ON _s3.n_regionkey = region.r_regionkey
JOIN _s7 AS _s7
  ON _s7.n_regionkey = region.r_regionkey
ORDER BY
  region.r_name
