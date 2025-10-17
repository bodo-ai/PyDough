SELECT
  MAX(nation.n_name) AS name,
  CAST((
    SUM((
      POWER(supplier.s_acctbal, 2)
    )) - (
      CAST((
        POWER(SUM(supplier.s_acctbal), 2)
      ) AS REAL) / COUNT(supplier.s_acctbal)
    )
  ) AS REAL) / COUNT(supplier.s_acctbal) AS var,
  POWER(
    (
      CAST((
        SUM((
          POWER(supplier.s_acctbal, 2)
        )) - (
          CAST((
            POWER(SUM(supplier.s_acctbal), 2)
          ) AS REAL) / COUNT(supplier.s_acctbal)
        )
      ) AS REAL) / COUNT(supplier.s_acctbal)
    ),
    0.5
  ) AS std,
  CAST((
    SUM((
      POWER(supplier.s_acctbal, 2)
    )) - (
      CAST((
        POWER(SUM(supplier.s_acctbal), 2)
      ) AS REAL) / COUNT(supplier.s_acctbal)
    )
  ) AS REAL) / (
    COUNT(supplier.s_acctbal) - 1
  ) AS sample_var,
  POWER(
    (
      CAST((
        SUM((
          POWER(supplier.s_acctbal, 2)
        )) - (
          CAST((
            POWER(SUM(supplier.s_acctbal), 2)
          ) AS REAL) / COUNT(supplier.s_acctbal)
        )
      ) AS REAL) / (
        COUNT(supplier.s_acctbal) - 1
      )
    ),
    0.5
  ) AS sample_std,
  CAST((
    SUM((
      POWER(supplier.s_acctbal, 2)
    )) - (
      CAST((
        POWER(SUM(supplier.s_acctbal), 2)
      ) AS REAL) / COUNT(supplier.s_acctbal)
    )
  ) AS REAL) / COUNT(supplier.s_acctbal) AS pop_var,
  POWER(
    (
      CAST((
        SUM((
          POWER(supplier.s_acctbal, 2)
        )) - (
          CAST((
            POWER(SUM(supplier.s_acctbal), 2)
          ) AS REAL) / COUNT(supplier.s_acctbal)
        )
      ) AS REAL) / COUNT(supplier.s_acctbal)
    ),
    0.5
  ) AS pop_std
FROM tpch.nation AS nation
JOIN tpch.supplier AS supplier
  ON nation.n_nationkey = supplier.s_nationkey
WHERE
  nation.n_name IN ('ALGERIA', 'ARGENTINA')
GROUP BY
  supplier.s_nationkey
