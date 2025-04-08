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
          DAY_OF_WEEK(payment_date) + 6
        ) % 7
      )
    ) IN (5, 6)
)
