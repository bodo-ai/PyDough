WITH _u_0 AS (
  SELECT
    customer_id AS _u_1
  FROM dealership.sales
  GROUP BY
    1
)
SELECT
  customers.id AS _id
FROM dealership.customers AS customers
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = customers.id
WHERE
  NOT _u_0._u_1 IS NULL
