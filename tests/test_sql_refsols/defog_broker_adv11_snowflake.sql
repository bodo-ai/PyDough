SELECT
  COUNT(*) AS n_customers
FROM main.sbcustomer
WHERE
  ENDSWITH(sbcustemail, '.com')
