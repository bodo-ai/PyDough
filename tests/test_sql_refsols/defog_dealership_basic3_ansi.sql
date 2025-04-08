WITH _t0 AS (
  SELECT
    sales._id AS _id,
    sales.salesperson_id AS salesperson_id
  FROM main.sales AS sales
), _t0_2 AS (
  SELECT
    payments_received.payment_method AS payment_method,
    payments_received.sale_id AS sale_id
  FROM main.payments_received AS payments_received
  WHERE
    payments_received.payment_method = 'cash'
), _t1 AS (
  SELECT
    _t0.sale_id AS sale_id
  FROM _t0_2 AS _t0
), _t3 AS (
  SELECT
    _t0.salesperson_id AS salesperson_id
  FROM _t0 AS _t0
  JOIN _t1 AS _t1
    ON _t0._id = _t1.sale_id
), _t2 AS (
  SELECT
    salespersons._id AS _id
  FROM main.salespersons AS salespersons
)
SELECT
  _t2._id AS salesperson_id
FROM _t2 AS _t2
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _t3 AS _t3
    WHERE
      _t2._id = _t3.salesperson_id
  )
