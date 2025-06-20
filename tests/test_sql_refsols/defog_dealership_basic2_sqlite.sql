SELECT
  _s0._id AS _id
FROM main.customers AS _s0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.sales AS _s1
    WHERE
      _s0._id = _s1.customer_id
  )
