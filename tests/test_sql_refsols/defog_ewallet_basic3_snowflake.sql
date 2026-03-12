SELECT
  mid AS merchant
FROM ewallet.merchants
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM ewallet.wallet_transactions_daily
    WHERE
      merchants.mid = receiver_id AND receiver_type = 1
  )
