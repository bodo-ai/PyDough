SELECT
  COUNT(*) AS num_customers_checking_accounts
FROM bodo.fsi.accounts
WHERE
  accounttype IN ('HPlnssRN', 'XADfRcm')
