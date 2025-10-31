WITH _s0 AS (
  SELECT
    a_branchkey,
    a_key
  FROM crbnk.accounts
), _t1 AS (
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
  COUNT(*) AS n
FROM _t1 AS _t1
JOIN _s0 AS _s2
  ON _t1.t_sourceaccount = CASE
    WHEN _s2.a_key = 0
    THEN 0
    ELSE CASE WHEN _s2.a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(_s2.a_key, 1 + INSTR(_s2.a_key, '-'), CAST(LENGTH(_s2.a_key) AS REAL) / 2) AS INTEGER)
  END
JOIN crbnk.branches AS branches
  ON _s2.a_branchkey = branches.b_key AND branches.b_name = 'Tucson University Branch'
WHERE
  _t1.cumavg > 50.0
