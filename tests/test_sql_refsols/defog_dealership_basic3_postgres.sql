WITH _u_0 AS (
  SELECT
    sales.salesperson_id AS _u_1
  FROM main.sales AS sales
  JOIN main.payments_received AS payments_received
    ON payments_received.payment_method = 'cash'
    AND payments_received.sale_id = sales._id
  GROUP BY
    sales.salesperson_id
)
SELECT
  salespersons._id AS salesperson_id
FROM main.salespersons AS salespersons
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = salespersons._id
WHERE
  NOT _u_0._u_1 IS NULL
