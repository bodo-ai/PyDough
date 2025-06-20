SELECT
  salespersons._id AS _id,
  salespersons.first_name AS first_name,
  salespersons.last_name AS last_name
FROM main.salespersons AS salespersons
JOIN main.sales AS sales
  ON sales.salesperson_id = salespersons._id
