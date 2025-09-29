WITH _s1 AS (
  SELECT
    accountid,
    SUM(amount) AS sum_amount
  FROM bodo.fsi.transactions
  GROUP BY
    1
)
SELECT
  accounts.accounttype AS acct_type,
  ROUND(COALESCE(_s1.sum_amount, 0) / SUM(COALESCE(_s1.sum_amount, 0)) OVER (), 2) AS pct_total_txn
FROM bodo.fsi.accounts AS accounts
JOIN _s1 AS _s1
  ON _s1.accountid = accounts.accountid
ORDER BY
  2 DESC NULLS LAST
LIMIT 5
