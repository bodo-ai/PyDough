SELECT
  COUNT(*) AS n_customers
FROM main.sbcustomer
WHERE
  (
    LOWER(sbcustname) LIKE '%ez' OR LOWER(sbcustname) LIKE 'j%'
  )
  AND LOWER(sbcuststate) LIKE '%a'
