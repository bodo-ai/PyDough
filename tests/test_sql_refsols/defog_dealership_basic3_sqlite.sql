SELECT
  _id AS salesperson_id
FROM (
  SELECT
    _id
  FROM main.salespersons
)
WHERE
  EXISTS(
    SELECT
      1
    FROM (
      SELECT
        salesperson_id
      FROM (
        SELECT
          _id,
          salesperson_id
        FROM main.sales
      )
      WHERE
        EXISTS(
          SELECT
            1
          FROM (
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
          WHERE
            _id = sale_id
        )
    )
    WHERE
      _id = salesperson_id
  )
