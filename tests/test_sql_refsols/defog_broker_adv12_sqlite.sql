SELECT
  COUNT() AS n_customers
FROM (
  SELECT
    _id
  FROM (
    SELECT
      sbCustId AS _id,
      sbCustName AS name,
      sbCustState AS state
    FROM main.sbCustomer
  )
  WHERE
    (
      (
        LOWER(name) LIKE 'j%'
      ) OR (
        LOWER(name) LIKE '%ez'
      )
    )
    AND (
      LOWER(state) LIKE '%a'
    )
)
