WITH _s1 AS (
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
    s_nationkey
  FROM tpch.supplier
  GROUP BY
    s_nationkey
)
SELECT
  nation.n_name AS name,
  _s1.pop_var AS var,
  _s1.pop_std AS std,
  _s1.sample_var,
  _s1.sample_std,
  _s1.pop_var,
  _s1.pop_std
FROM tpch.nation AS nation
JOIN _s1 AS _s1
  ON _s1.s_nationkey = nation.n_nationkey
WHERE
  nation.n_name IN ('ALGERIA', 'ARGENTINA')
