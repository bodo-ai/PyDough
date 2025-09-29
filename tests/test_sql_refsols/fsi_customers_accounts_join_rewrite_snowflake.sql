SELECT
  COUNT(*) AS num_customers_checking_accounts
FROM bodo.fsi.protected_customers AS protected_customers
JOIN bodo.fsi.accounts AS accounts
  ON accounts.accounttype <> 'checking'
  AND accounts.customerid = protected_customers.customerid
