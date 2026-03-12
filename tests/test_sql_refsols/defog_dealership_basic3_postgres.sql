SELECT
  _id AS salesperson_id
FROM main.salespersons
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.sales AS sales
    JOIN main.payments_received AS payments_received
      ON payments_received.payment_method = 'cash'
      AND payments_received.sale_id = sales._id
    WHERE
      sales.salesperson_id = salespersons._id
  )
