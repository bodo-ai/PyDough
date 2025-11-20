WITH _s1 AS (
  SELECT
    s_nationkey,
    POWER(
      (
        (
          SUM((
            POWER(s_acctbal, 2)
          )) - (
            (
              POWER(SUM(s_acctbal), 2)
            ) / COUNT(s_acctbal)
          )
        ) / COUNT(s_acctbal)
      ),
      0.5
    ) AS population_std_s_acctbal,
    (
      SUM((
        POWER(s_acctbal, 2)
      )) - (
        (
          POWER(SUM(s_acctbal), 2)
        ) / COUNT(s_acctbal)
      )
    ) / COUNT(s_acctbal) AS population_var_s_acctbal,
    POWER(
      (
        (
          SUM((
            POWER(s_acctbal, 2)
          )) - (
            (
              POWER(SUM(s_acctbal), 2)
            ) / COUNT(s_acctbal)
          )
        ) / (
          COUNT(s_acctbal) - 1
        )
      ),
      0.5
    ) AS sample_std_s_acctbal,
    (
      SUM((
        POWER(s_acctbal, 2)
      )) - (
        (
          POWER(SUM(s_acctbal), 2)
        ) / COUNT(s_acctbal)
      )
    ) / (
      COUNT(s_acctbal) - 1
    ) AS sample_var_s_acctbal
  FROM tpch.SUPPLIER
  GROUP BY
    1
)
SELECT
  NATION.n_name AS name,
  _s1.population_var_s_acctbal AS var,
  _s1.population_std_s_acctbal AS std,
  _s1.sample_var_s_acctbal AS sample_var,
  _s1.sample_std_s_acctbal AS sample_std,
  _s1.population_var_s_acctbal AS pop_var,
  _s1.population_std_s_acctbal AS pop_std
FROM tpch.NATION AS NATION
JOIN _s1 AS _s1
  ON NATION.n_nationkey = _s1.s_nationkey
WHERE
  NATION.n_name IN ('ALGERIA', 'ARGENTINA')
