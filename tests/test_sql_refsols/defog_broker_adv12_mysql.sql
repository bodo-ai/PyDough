SELECT
  COUNT(*) AS n_customers
FROM broker.sbCustomer
WHERE
  (
    LOWER(sbCustName) LIKE '%ez' OR LOWER(sbCustName) LIKE 'j%'
  )
  AND LOWER(sbCustState) LIKE '%a'
