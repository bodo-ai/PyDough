SELECT
  (
    SUM((
      POWER(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END, 2)
    )) - (
      (
        POWER(SUM(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END), 2)
      ) / COUNT(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END)
    )
  ) / (
    COUNT(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END) - 1
  ) AS var_samp_0_nnull,
  (
    SUM((
      POWER(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END, 2)
    )) - (
      (
        POWER(SUM(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END), 2)
      ) / COUNT(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END)
    )
  ) / (
    COUNT(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END) - 1
  ) AS var_samp_1_nnull,
  (
    SUM((
      POWER(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END, 2)
    )) - (
      (
        POWER(SUM(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END), 2)
      ) / COUNT(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END)
    )
  ) / (
    COUNT(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END) - 1
  ) AS var_samp_2_nnull,
  (
    SUM((
      POWER(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END, 2)
    )) - (
      (
        POWER(SUM(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END), 2)
      ) / COUNT(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END)
    )
  ) / COUNT(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END) AS var_pop_0_nnull,
  (
    SUM((
      POWER(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END, 2)
    )) - (
      (
        POWER(SUM(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END), 2)
      ) / COUNT(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END)
    )
  ) / COUNT(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END) AS var_pop_1_nnull,
  (
    SUM((
      POWER(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END, 2)
    )) - (
      (
        POWER(SUM(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END), 2)
      ) / COUNT(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END)
    )
  ) / COUNT(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END) AS var_pop_2_nnull,
  POWER(
    (
      (
        SUM((
          POWER(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END, 2)
        )) - (
          (
            POWER(SUM(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END), 2)
          ) / COUNT(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END)
        )
      ) / (
        COUNT(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END) - 1
      )
    ),
    0.5
  ) AS std_samp_0_nnull,
  POWER(
    (
      (
        SUM((
          POWER(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END, 2)
        )) - (
          (
            POWER(SUM(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END), 2)
          ) / COUNT(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END)
        )
      ) / (
        COUNT(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END) - 1
      )
    ),
    0.5
  ) AS std_samp_1_nnull,
  POWER(
    (
      (
        SUM((
          POWER(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END, 2)
        )) - (
          (
            POWER(SUM(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END), 2)
          ) / COUNT(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END)
        )
      ) / (
        COUNT(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END) - 1
      )
    ),
    0.5
  ) AS std_samp_2_nnull,
  POWER(
    (
      (
        SUM((
          POWER(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END, 2)
        )) - (
          (
            POWER(SUM(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END), 2)
          ) / COUNT(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END)
        )
      ) / COUNT(CASE WHEN c_custkey > 3 THEN c_acctbal ELSE NULL END)
    ),
    0.5
  ) AS std_pop_0_nnull,
  POWER(
    (
      (
        SUM((
          POWER(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END, 2)
        )) - (
          (
            POWER(SUM(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END), 2)
          ) / COUNT(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END)
        )
      ) / COUNT(CASE WHEN c_custkey > 2 THEN c_acctbal ELSE NULL END)
    ),
    0.5
  ) AS std_pop_1_nnull,
  POWER(
    (
      (
        SUM((
          POWER(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END, 2)
        )) - (
          (
            POWER(SUM(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END), 2)
          ) / COUNT(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END)
        )
      ) / COUNT(CASE WHEN c_custkey > 1 THEN c_acctbal ELSE NULL END)
    ),
    0.5
  ) AS std_pop_2_nnull
FROM tpch.CUSTOMER
WHERE
  c_custkey IN (1, 2, 3)
