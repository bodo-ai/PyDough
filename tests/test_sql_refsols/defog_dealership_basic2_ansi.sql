WITH _s1 AS (
  SELECT
    customer_id
  FROM main.sales
)
SELECT
  customers._id
FROM main.customers AS customers
SEMI JOIN _s1 AS _s1
  ON _s1.customer_id = customers._id
