WITH _s0 AS (
  SELECT
    sales._id AS _id,
    sales.salesperson_id AS salesperson_id
  FROM main.sales AS sales
), _t0 AS (
  SELECT
    payments_received.payment_method AS payment_method,
    payments_received.sale_id AS sale_id
  FROM main.payments_received AS payments_received
  WHERE
    payments_received.payment_method = 'cash'
), _s1 AS (
  SELECT
    _t0.sale_id AS sale_id
  FROM _t0 AS _t0
), _s3 AS (
  SELECT
    _s0.salesperson_id AS salesperson_id
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0._id = _s1.sale_id
), _s2 AS (
  SELECT
    salespersons._id AS _id,
    salespersons._id AS salesperson_id
  FROM main.salespersons AS salespersons
)
SELECT
  _s2.salesperson_id AS salesperson_id
FROM _s2 AS _s2
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _s3 AS _s3
    WHERE
      _s2._id = _s3.salesperson_id
  )
