SELECT
  ANY_VALUE(NATION.n_name) AS name,
  (
    SUM((
      POWER(SUPPLIER.s_acctbal, 2)
    )) - (
      (
        POWER(SUM(SUPPLIER.s_acctbal), 2)
      ) / COUNT(SUPPLIER.s_acctbal)
    )
  ) / COUNT(SUPPLIER.s_acctbal) AS var,
  POWER(
    (
      (
        SUM((
          POWER(SUPPLIER.s_acctbal, 2)
        )) - (
          (
            POWER(SUM(SUPPLIER.s_acctbal), 2)
          ) / COUNT(SUPPLIER.s_acctbal)
        )
      ) / COUNT(SUPPLIER.s_acctbal)
    ),
    0.5
  ) AS std,
  (
    SUM((
      POWER(SUPPLIER.s_acctbal, 2)
    )) - (
      (
        POWER(SUM(SUPPLIER.s_acctbal), 2)
      ) / COUNT(SUPPLIER.s_acctbal)
    )
  ) / (
    COUNT(SUPPLIER.s_acctbal) - 1
  ) AS sample_var,
  POWER(
    (
      (
        SUM((
          POWER(SUPPLIER.s_acctbal, 2)
        )) - (
          (
            POWER(SUM(SUPPLIER.s_acctbal), 2)
          ) / COUNT(SUPPLIER.s_acctbal)
        )
      ) / (
        COUNT(SUPPLIER.s_acctbal) - 1
      )
    ),
    0.5
  ) AS sample_std,
  (
    SUM((
      POWER(SUPPLIER.s_acctbal, 2)
    )) - (
      (
        POWER(SUM(SUPPLIER.s_acctbal), 2)
      ) / COUNT(SUPPLIER.s_acctbal)
    )
  ) / COUNT(SUPPLIER.s_acctbal) AS pop_var,
  POWER(
    (
      (
        SUM((
          POWER(SUPPLIER.s_acctbal, 2)
        )) - (
          (
            POWER(SUM(SUPPLIER.s_acctbal), 2)
          ) / COUNT(SUPPLIER.s_acctbal)
        )
      ) / COUNT(SUPPLIER.s_acctbal)
    ),
    0.5
  ) AS pop_std
FROM tpch.NATION AS NATION
JOIN tpch.SUPPLIER AS SUPPLIER
  ON NATION.n_nationkey = SUPPLIER.s_nationkey
WHERE
  NATION.n_name IN ('ALGERIA', 'ARGENTINA')
GROUP BY
  SUPPLIER.s_nationkey
