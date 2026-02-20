SELECT
  COUNT(*) AS n_customers
FROM broker.sbCustomer
WHERE
  (
    LOWER(sbcustname) LIKE '%ez' OR LOWER(sbcustname) LIKE 'j%'
  )
  AND LOWER(sbcuststate) LIKE '%a'
