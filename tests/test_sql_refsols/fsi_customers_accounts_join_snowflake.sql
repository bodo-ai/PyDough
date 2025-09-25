SELECT
  COUNT(*) AS num_customers_checking_accounts
FROM bodo.fsi.customers AS customers
JOIN bodo.fsi.accounts AS accounts
  ON accounts.accounttype <> 'checking' AND accounts.customerid = customers.customerid
