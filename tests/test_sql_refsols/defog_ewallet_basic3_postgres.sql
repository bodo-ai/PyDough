SELECT
  mid AS merchant
FROM main.merchants
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.wallet_transactions_daily
    WHERE
      merchants.mid = receiver_id AND receiver_type = 1
  )
