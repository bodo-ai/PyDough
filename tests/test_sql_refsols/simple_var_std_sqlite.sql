WITH _s1 AS (
  SELECT
    CAST((
      SUM((
        POWER(s_acctbal, 2)
      )) - (
        CAST((
          POWER(SUM(s_acctbal), 2)
        ) AS REAL) / COUNT(s_acctbal)
      )
    ) AS REAL) / COUNT(s_acctbal) AS agg_1,
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
    ) AS agg_3,
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
    ) AS agg_0,
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
    ) AS agg_2,
    s_nationkey AS nation_key
  FROM tpch.supplier
  GROUP BY
    s_nationkey
)
SELECT
  nation.n_name AS name,
  _s1.agg_1 AS var,
  _s1.agg_0 AS std,
  _s1.agg_3 AS sample_var,
  _s1.agg_2 AS sample_std,
  _s1.agg_1 AS pop_var,
  _s1.agg_0 AS pop_std
FROM tpch.nation AS nation
LEFT JOIN _s1 AS _s1
  ON _s1.nation_key = nation.n_nationkey
WHERE
  nation.n_name IN ('ALGERIA', 'ARGENTINA')
