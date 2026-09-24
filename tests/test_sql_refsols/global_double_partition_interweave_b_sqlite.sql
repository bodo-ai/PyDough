WITH _s2 AS (
  SELECT
    AVG(c_acctbal) AS avg_c_acctbal
  FROM tpch.customer
), _s0 AS (
  SELECT
    c_mktsegment,
    c_nationkey,
    COUNT(c_acctbal) AS count_c_acctbal,
    SUM(c_acctbal) AS sum_c_acctbal
  FROM tpch.customer
  GROUP BY
    1,
    2
), _s3 AS (
  SELECT
    _s0.c_mktsegment,
    SUM(_s0.count_c_acctbal) AS sum_count_c_acctbal,
    SUM(_s0.sum_c_acctbal) AS sum_sum_c_acctbal
  FROM _s0 AS _s0
  JOIN tpch.nation AS nation
    ON _s0.c_nationkey = nation.n_nationkey
  GROUP BY
    nation.n_name,
    1
)
SELECT
  _s3.c_mktsegment AS market_segment,
  ROUND(
    MAX(
      CAST((
        100.0 * (
          (
            CAST(_s3.sum_sum_c_acctbal AS REAL) / _s3.sum_count_c_acctbal
          ) - _s2.avg_c_acctbal
        )
      ) AS REAL) / _s2.avg_c_acctbal
    ),
    2
  ) AS max_nation_pct_diff
FROM _s2 AS _s2
CROSS JOIN _s3 AS _s3
GROUP BY
  1
