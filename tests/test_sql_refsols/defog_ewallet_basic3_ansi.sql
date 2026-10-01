WITH _s1 AS (
  SELECT
    receiver_id
  FROM main.wallet_transactions_daily
  WHERE
    receiver_type = 1
)
SELECT
  merchants.mid AS merchant
FROM main.merchants AS merchants
SEMI JOIN _s1 AS _s1
  ON _s1.receiver_id = merchants.mid
