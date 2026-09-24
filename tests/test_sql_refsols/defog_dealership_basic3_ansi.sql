WITH _s3 AS (
  SELECT
    sales.salesperson_id
  FROM main.sales AS sales
  JOIN main.payments_received AS payments_received
    ON payments_received.payment_method = 'cash'
    AND payments_received.sale_id = sales._id
)
SELECT
  salespersons._id AS salesperson_id
FROM main.salespersons AS salespersons
SEMI JOIN _s3 AS _s3
  ON _s3.salesperson_id = salespersons._id
