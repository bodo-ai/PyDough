SELECT
  id AS salesperson_id
FROM dealership.salespersons
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM dealership.sales AS sales
    JOIN dealership.payments_received AS payments_received
      ON payments_received.payment_method = 'cash'
      AND payments_received.sale_id = sales.id
    WHERE
      sales.salesperson_id = salespersons.id
  )
