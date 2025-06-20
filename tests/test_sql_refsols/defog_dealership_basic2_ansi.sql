SELECT
  customers._id AS _id
FROM main.customers AS customers
JOIN main.sales AS sales
  ON customers._id = sales.customer_id
