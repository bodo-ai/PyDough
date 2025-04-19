WITH _s1 AS (
  SELECT
    sales.customer_id AS customer_id
  FROM main.sales AS sales
), _s0 AS (
  SELECT
    customers._id AS _id
  FROM main.customers AS customers
)
SELECT
  _s0._id AS _id
FROM _s0 AS _s0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _s1 AS _s1
    WHERE
      _s0._id = _s1.customer_id
  )
