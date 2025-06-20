SELECT
  _s0._id AS _id,
  _s0.first_name AS first_name,
  _s0.last_name AS last_name
FROM main.salespersons AS _s0
JOIN main.sales AS _s1
  ON _s0._id = _s1.salesperson_id
