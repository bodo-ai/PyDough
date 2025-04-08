WITH _t1 AS (
  SELECT
    sales.customer_id AS customer_id
  FROM main.sales AS sales
), _t0 AS (
  SELECT
    customers._id AS _id
  FROM main.customers AS customers
)
SELECT
  _t0._id AS _id
FROM _t0 AS _t0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _t1 AS _t1
    WHERE
      _t0._id = _t1.customer_id
  )
