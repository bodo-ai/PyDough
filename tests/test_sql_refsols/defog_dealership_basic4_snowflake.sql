SELECT
  id AS _id,
  first_name,
  last_name
FROM dealership.salespersons
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM dealership.sales
    WHERE
      salesperson_id = salespersons.id
  )
