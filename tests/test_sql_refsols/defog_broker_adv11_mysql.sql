SELECT
  COUNT(*) AS n_customers
FROM main.sbCustomer
WHERE
  sbcustemail LIKE '%.com'
