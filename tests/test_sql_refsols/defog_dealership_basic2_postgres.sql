WITH _u_0 AS (
  SELECT
    customer_id AS _u_1
  FROM main.sales
  GROUP BY
    customer_id
)
SELECT
  customers._id
FROM main.customers AS customers
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = customers._id
WHERE
  NOT _u_0._u_1 IS NULL
