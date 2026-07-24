SELECT
  COUNT(*) AS n_customers
FROM mongo.defog.sbcustomer
WHERE
  (
    LOWER(sbcustname) LIKE '%ez' OR STARTS_WITH(LOWER(sbcustname), 'j')
  )
  AND LOWER(sbcuststate) LIKE '%a'
