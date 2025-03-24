SELECT
  mid AS merchant
FROM (
  SELECT
    mid
  FROM main.merchants
)
WHERE
  EXISTS(
    SELECT
      1
    FROM (
      SELECT
        receiver_id
      FROM (
        SELECT
          receiver_id,
          receiver_type
        FROM main.wallet_transactions_daily
      )
      WHERE
        receiver_type = 1
    )
    WHERE
      mid = receiver_id
  )
