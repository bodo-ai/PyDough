SELECT
  _s0._id AS _id,
  _s0.first_name AS first_name,
  _s0.last_name AS last_name
FROM main.salespersons AS _s0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM main.sales AS _s1
    WHERE
      _s0._id = _s1.salesperson_id
  )
