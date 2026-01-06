SELECT
  COUNT(*) AS n
FROM bodo.fsi.accounts AS accounts
JOIN bodo.fsi.protected_customers AS protected_customers
  ON PTY_UNPROTECT(protected_customers.customerid, 'deAccount') = PTY_UNPROTECT_ACCOUNT(accounts.customerid)
  AND protected_customers.state = 'V6kSQBaqGv'
WHERE
  accounts.balance < 20000 AND accounts.currency IN ('jpb', 'gFr')
