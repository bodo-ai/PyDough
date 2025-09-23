SELECT
  COUNT(*) AS n_customers
FROM main.sbcustomer
WHERE
  sbcustemail LIKE '%.com'
