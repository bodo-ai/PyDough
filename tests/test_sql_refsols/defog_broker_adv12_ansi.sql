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
  ) AS _t1
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
) AS _t0
