SELECT
  COUNT(*) AS num_customers_checking_accounts
FROM bodo.fsi.accounts
WHERE
  accounttype <> PTY_PROTECT('checking', 'deAccount')
