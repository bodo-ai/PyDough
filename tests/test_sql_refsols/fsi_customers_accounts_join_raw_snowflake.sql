SELECT
  COUNT(*) AS num_customers_checking_accounts
FROM bodo.fsi.protected_customers AS protected_customers
JOIN bodo.fsi.accounts AS accounts
  ON PTY_UNPROTECT(accounts.accounttype, 'deAccount') <> 'checking'
  AND PTY_UNPROTECT(protected_customers.customerid, 'deAccount') = PTY_UNPROTECT_ACCOUNT(accounts.customerid)
