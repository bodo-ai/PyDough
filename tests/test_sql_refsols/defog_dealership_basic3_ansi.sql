SELECT
  _id AS salesperson_id
FROM (
  SELECT
    _id
  FROM main.salespersons
)
SEMI JOIN (
  SELECT
    salesperson_id
  FROM (
    SELECT
      _id,
      salesperson_id
    FROM main.sales
  )
  INNER JOIN (
    SELECT
      sale_id
    FROM (
      SELECT
        payment_method,
        sale_id
      FROM main.payments_received
    )
    WHERE
      payment_method = 'cash'
  )
    ON _id = sale_id
)
  ON _id = salesperson_id
