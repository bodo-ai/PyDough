SELECT
  _id
FROM main.customers
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.sales
    WHERE
      customers._id = customer_id
  )
