SELECT
  _s0._id AS _id
FROM main.customers AS _s0
JOIN main.sales AS _s1
  ON _s0._id = _s1.customer_id
