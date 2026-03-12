SELECT
  id AS _id
FROM dealership.customers
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM dealership.sales
    WHERE
      customers.id = customer_id
  )
