WITH _s0 AS (
  SELECT
    a_branchkey,
    a_key
  FROM crbnk.accounts
), _s4 AS (
  SELECT
    transactions.t_sourceaccount,
    AVG(POWER((
      1025.67 - transactions.t_amount
    ), 0.5)) OVER (PARTITION BY _s0.a_branchkey ORDER BY DATETIME(transactions.t_ts, '+54321 seconds') ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumavg
  FROM _s0 AS _s0
  JOIN crbnk.transactions AS transactions
    ON transactions.t_destaccount = CASE
      WHEN _s0.a_key = 0
      THEN 0
      ELSE CASE WHEN _s0.a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(_s0.a_key, 1 + INSTR(_s0.a_key, '-'), CAST(LENGTH(_s0.a_key) AS REAL) / 2) AS INTEGER)
    END
)
SELECT
  ROUND(CAST(_s4.cumavg AS REAL) / 10.0, 0) * 10 AS bucket,
  COUNT(*) AS n
FROM _s4 AS _s4
JOIN _s0 AS _s2
  ON _s4.t_sourceaccount = CASE
    WHEN _s2.a_key = 0
    THEN 0
    ELSE CASE WHEN _s2.a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(_s2.a_key, 1 + INSTR(_s2.a_key, '-'), CAST(LENGTH(_s2.a_key) AS REAL) / 2) AS INTEGER)
  END
JOIN crbnk.branches AS branches
  ON _s2.a_branchkey = branches.b_key AND branches.b_name = 'Tucson University Branch'
GROUP BY
  1
