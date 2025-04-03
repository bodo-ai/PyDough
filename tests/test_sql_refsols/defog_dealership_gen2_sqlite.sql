SELECT
  COUNT() AS weekend_payments
FROM (
  SELECT
    _id
  FROM (
    SELECT
      _id,
      payment_date,
      vendor_name
    FROM main.payments_made
  )
  WHERE
    (
      vendor_name = 'Utility Company'
    )
    AND (
      (
        (
          (
            (
              CAST(STRFTIME('%w', payment_date) AS INTEGER) + 6
            ) % 7
          )
        ) = 5
      )
      OR (
        (
          (
            (
              CAST(STRFTIME('%w', payment_date) AS INTEGER) + 6
            ) % 7
          )
        ) = 6
      )
    )
)
