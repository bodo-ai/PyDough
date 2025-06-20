SELECT
  _s0._id AS _id,
  _s0.make AS make,
  _s0.model AS model,
  _s0.year AS year
FROM main.cars AS _s0
JOIN main.sales AS _s1
  ON _s0._id = _s1.car_id
