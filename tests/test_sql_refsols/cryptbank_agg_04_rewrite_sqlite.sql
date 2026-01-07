WITH _t0 AS (
  SELECT
    a_branchkey,
    SUM(SQRT(a_balance)) AS sum_unmask_a_balance
  FROM crbnk.accounts
  GROUP BY
    1
)
SELECT
  a_branchkey AS branch_key,
  ROUND(
    CAST(COALESCE(sum_unmask_a_balance, 0) AS REAL) / SUM(COALESCE(sum_unmask_a_balance, 0)) OVER (),
    2
  ) AS pct_total_wealth
FROM _t0
