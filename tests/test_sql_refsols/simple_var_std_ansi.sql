WITH _s3 AS (
  SELECT
    STDDEV_POP(s_acctbal) AS pop_std,
    VARIANCE_POP(s_acctbal) AS pop_var,
    STDDEV(s_acctbal) AS sample_std,
    VARIANCE(s_acctbal) AS sample_var,
    STDDEV_POP(s_acctbal) AS std,
    VARIANCE_POP(s_acctbal) AS var,
    s_nationkey AS nation_key
  FROM tpch.supplier
  GROUP BY
    s_nationkey
)
SELECT
  _s0.n_name AS name,
  _s3.var,
  _s3.std,
  _s3.sample_var,
  _s3.sample_std,
  _s3.pop_var,
  _s3.pop_std
FROM tpch.nation AS _s0
JOIN _s3 AS _s3
  ON _s0.n_nationkey = _s3.nation_key
WHERE
  _s0.n_name IN ('ALGERIA', 'ARGENTINA')
