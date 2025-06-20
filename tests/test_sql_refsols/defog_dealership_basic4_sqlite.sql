WITH _u_0 AS (
  SELECT
    sales.salesperson_id AS _u_1
  FROM main.sales AS sales
  GROUP BY
    sales.salesperson_id
)
SELECT
  salespersons._id AS _id,
  salespersons.first_name AS first_name,
  salespersons.last_name AS last_name
FROM main.salespersons AS salespersons
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = salespersons._id
WHERE
  _u_0._u_1 IS NULL
