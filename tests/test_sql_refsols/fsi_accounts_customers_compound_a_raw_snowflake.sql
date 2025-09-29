SELECT
  COUNT(*) AS n
FROM bodo.fsi.accounts AS accounts
JOIN bodo.fsi.protected_customers AS protected_customers
  ON PTY_UNPROTECT(protected_customers.customerid, 'account') = PTY_UNPROTECT_ACCOUNT(accounts.customerid)
  AND PTY_UNPROTECT(protected_customers.state, 'address') = 'California'
WHERE
  PTY_UNPROTECT_ACCOUNT(accounts.currency) <> 'GBP' AND accounts.balance < 20000
