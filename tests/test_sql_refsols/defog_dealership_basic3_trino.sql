WITH _u_0 AS (
  SELECT
    sales.salesperson_id AS _u_1
  FROM cassandra.defog.sales AS sales
  JOIN mongo.defog.payments_received AS payments_received
    ON payments_received.payment_method = 'cash'
    AND payments_received.sale_id = sales._id
  GROUP BY
    1
)
SELECT
  salespersons._id AS salesperson_id
FROM mongo.defog.salespersons AS salespersons
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = salespersons._id
WHERE
  NOT _u_0._u_1 IS NULL
