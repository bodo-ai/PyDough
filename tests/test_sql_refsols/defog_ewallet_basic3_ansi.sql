SELECT
  mid AS merchant
FROM (
  SELECT
    mid
  FROM main.merchants
)
SEMI JOIN (
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
  ON mid = receiver_id
