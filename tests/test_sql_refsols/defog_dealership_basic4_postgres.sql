WITH _u_0 AS (
  SELECT
    salesperson_id AS _u_1
  FROM main.sales
  GROUP BY
    salesperson_id
)
SELECT
  salespersons._id,
  salespersons.first_name,
  salespersons.last_name
FROM main.salespersons AS salespersons
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = salespersons._id
WHERE
  _u_0._u_1 IS NULL
