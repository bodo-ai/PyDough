WITH _s3 AS (
  SELECT
    POWER(
      (
        CAST((
          SUM((
            POWER(s_acctbal, 2)
          )) - (
            CAST((
              POWER(SUM(s_acctbal), 2)
            ) AS REAL) / COUNT(s_acctbal)
          )
        ) AS REAL) / COUNT(s_acctbal)
      ),
      0.5
    ) AS pop_std,
    CAST((
      SUM((
        POWER(s_acctbal, 2)
      )) - (
        CAST((
          POWER(SUM(s_acctbal), 2)
        ) AS REAL) / COUNT(s_acctbal)
      )
    ) AS REAL) / COUNT(s_acctbal) AS pop_var,
    POWER(
      (
        CAST((
          SUM((
            POWER(s_acctbal, 2)
          )) - (
            CAST((
              POWER(SUM(s_acctbal), 2)
            ) AS REAL) / COUNT(s_acctbal)
          )
        ) AS REAL) / (
          COUNT(s_acctbal) - 1
        )
      ),
      0.5
    ) AS sample_std,
    CAST((
      SUM((
        POWER(s_acctbal, 2)
      )) - (
        CAST((
          POWER(SUM(s_acctbal), 2)
        ) AS REAL) / COUNT(s_acctbal)
      )
    ) AS REAL) / (
      COUNT(s_acctbal) - 1
    ) AS sample_var,
    POWER(
      (
        CAST((
          SUM((
            POWER(s_acctbal, 2)
          )) - (
            CAST((
              POWER(SUM(s_acctbal), 2)
            ) AS REAL) / COUNT(s_acctbal)
          )
        ) AS REAL) / COUNT(s_acctbal)
      ),
      0.5
    ) AS std,
    CAST((
      SUM((
        POWER(s_acctbal, 2)
      )) - (
        CAST((
          POWER(SUM(s_acctbal), 2)
        ) AS REAL) / COUNT(s_acctbal)
      )
    ) AS REAL) / COUNT(s_acctbal) AS var,
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
