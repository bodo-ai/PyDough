WITH _s1 AS (
  SELECT
    amount,
    receiver_id
  FROM main.wallet_transactions_daily
  WHERE
    created_at >= CAST(DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '150' DAY) AS DATE)
    AND receiver_type = 1
)
SELECT
  ANY_VALUE(merchants.name) AS merchant_name,
  COALESCE(CASE WHEN COUNT(_s1.receiver_id) <> 0 THEN COUNT(_s1.receiver_id) ELSE NULL END, 0) AS total_transactions,
  COALESCE(SUM(_s1.amount), 0) AS total_amount
FROM main.merchants AS merchants
LEFT JOIN _s1 AS _s1
  ON _s1.receiver_id = merchants.mid
GROUP BY
  merchants.mid
ORDER BY
  3 DESC
LIMIT 2
