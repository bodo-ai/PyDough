WITH _u_0 AS (
  SELECT
    wallet_transactions_daily.receiver_id AS _u_1
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  WHERE
    wallet_transactions_daily.receiver_type = 1
  GROUP BY
    wallet_transactions_daily.receiver_id
)
SELECT
  merchants.mid AS merchant
FROM main.merchants AS merchants
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = merchants.mid
WHERE
  NOT _u_0._u_1 IS NULL
