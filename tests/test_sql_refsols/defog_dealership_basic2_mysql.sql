SELECT
  _id
FROM dealership.customers
WHERE
  EXISTS(
    SELECT
      1 AS `1`
    FROM dealership.sales
    WHERE
      customers._id = customer_id
  )
