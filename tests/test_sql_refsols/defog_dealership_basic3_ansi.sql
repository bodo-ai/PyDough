SELECT
  salespersons._id AS salesperson_id
FROM main.salespersons AS salespersons
JOIN main.sales AS sales
  ON sales.salesperson_id = salespersons._id
JOIN main.payments_received AS payments_received
  ON payments_received.payment_method = 'cash'
  AND payments_received.sale_id = sales._id
