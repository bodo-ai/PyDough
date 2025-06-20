WITH _S1 AS (
  SELECT
    STDDEV_POP(s_acctbal) AS POP_STD,
    VARIANCE_POP(s_acctbal) AS POP_VAR,
    STDDEV(s_acctbal) AS SAMPLE_STD,
    VARIANCE(s_acctbal) AS SAMPLE_VAR,
    STDDEV_POP(s_acctbal) AS STD,
    VARIANCE_POP(s_acctbal) AS VAR,
    s_nationkey AS NATION_KEY
  FROM TPCH.SUPPLIER
  GROUP BY
    s_nationkey
)
SELECT
  NATION.n_name AS name,
  _S1.VAR AS var,
  _S1.STD AS std,
  _S1.SAMPLE_VAR AS sample_var,
  _S1.SAMPLE_STD AS sample_std,
  _S1.POP_VAR AS pop_var,
  _S1.POP_STD AS pop_std
FROM TPCH.NATION AS NATION
JOIN _S1 AS _S1
  ON NATION.n_nationkey = _S1.NATION_KEY
WHERE
  NATION.n_name IN ('ALGERIA', 'ARGENTINA')
