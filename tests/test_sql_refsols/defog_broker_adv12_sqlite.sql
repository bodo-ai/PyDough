SELECT
  COUNT() AS n_customers
FROM main.sbcustomer AS sbcustomer
WHERE
  (
    LOWER(sbcustomer.sbcustname) LIKE '%ez' OR LOWER(sbcustomer.sbcustname) LIKE 'j%'
  )
  AND LOWER(sbcustomer.sbcuststate) LIKE '%a'
