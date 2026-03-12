SELECT
  _id,
  first_name,
  last_name
FROM main.salespersons
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM main.sales
    WHERE
      salesperson_id = salespersons._id
  )
