WITH _s1 AS (
  SELECT
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
    ) AS pop_std,
    (
      SUM((
        POWER(s_acctbal, 2)
      )) - (
        (
          POWER(SUM(s_acctbal), 2)
        ) / COUNT(s_acctbal)
      )
    ) / COUNT(s_acctbal) AS pop_var,
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
    ) AS sample_std,
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
    ) AS sample_var,
    s_nationkey
  FROM tpch.SUPPLIER
  GROUP BY
    5
)
SELECT
  NATION.n_name AS name,
  _s1.pop_var AS var,
  _s1.pop_std AS std,
  _s1.sample_var,
  _s1.sample_std,
  _s1.pop_var,
  _s1.pop_std
FROM tpch.NATION AS NATION
JOIN _s1 AS _s1
  ON NATION.n_nationkey = _s1.s_nationkey
WHERE
  NATION.n_name IN ('ALGERIA', 'ARGENTINA')
