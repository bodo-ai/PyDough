SELECT
  COUNT(*) AS n_customers
FROM MAIN.SBCUSTOMER
WHERE
  (
    ENDSWITH(LOWER(sbcustname), 'ez') OR STARTSWITH(LOWER(sbcustname), 'j')
  )
  AND ENDSWITH(LOWER(sbcuststate), 'a')
