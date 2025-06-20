WITH _u_0 AS (
  SELECT
    sales.customer_id AS _u_1
  FROM main.sales AS sales
  GROUP BY
    sales.customer_id
)
SELECT
  customers._id AS _id
FROM main.customers AS customers
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = customers._id
WHERE
  NOT _u_0._u_1 IS NULL
