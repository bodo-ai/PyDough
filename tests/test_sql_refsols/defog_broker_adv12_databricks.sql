SELECT
  COUNT(*) AS n_customers
FROM defog.broker.sbcustomer
WHERE
  (
    ENDSWITH(LOWER(sbcustname), 'ez') OR STARTSWITH(LOWER(sbcustname), 'j')
  )
  AND ENDSWITH(LOWER(sbcuststate), 'a')
