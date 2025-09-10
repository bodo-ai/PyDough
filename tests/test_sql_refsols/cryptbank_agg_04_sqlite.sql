WITH _s1 AS (
  SELECT
    a_branchkey,
    SUM(a_balance) AS sum_a_balance
  FROM crbnk.accounts
  GROUP BY
    1
)
SELECT
  branches.b_key AS branch_key,
  ROUND(
    CAST(COALESCE(_s1.sum_a_balance, 0) AS REAL) / SUM(COALESCE(_s1.sum_a_balance, 0)) OVER (),
    2
  ) AS pct_total_wealth
FROM crbnk.branches AS branches
JOIN _s1 AS _s1
  ON _s1.a_branchkey = branches.b_key
