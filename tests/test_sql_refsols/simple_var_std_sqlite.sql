WITH _s1 AS (
  SELECT
    s_nationkey,
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
    ) AS population_std_sacctbal,
    CAST((
      SUM((
        POWER(s_acctbal, 2)
      )) - (
        CAST((
          POWER(SUM(s_acctbal), 2)
        ) AS REAL) / COUNT(s_acctbal)
      )
    ) AS REAL) / COUNT(s_acctbal) AS population_var_sacctbal,
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
    ) AS sample_std_sacctbal,
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
    ) AS sample_var_sacctbal
  FROM tpch.supplier
  GROUP BY
    1
)
SELECT
  nation.n_name AS name,
  _s1.population_var_sacctbal AS var,
  _s1.population_std_sacctbal AS std,
  _s1.sample_var_sacctbal AS sample_var,
  _s1.sample_std_sacctbal AS sample_std,
  _s1.population_var_sacctbal AS pop_var,
  _s1.population_std_sacctbal AS pop_std
FROM tpch.nation AS nation
JOIN _s1 AS _s1
  ON _s1.s_nationkey = nation.n_nationkey
WHERE
  nation.n_name IN ('ALGERIA', 'ARGENTINA')
