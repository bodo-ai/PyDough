WITH _s1 AS (
  SELECT
    amount,
    receiver_id
  FROM ewallet.wallet_transactions_daily
  WHERE
    created_at >= DATE_TRUNC(
      'DAY',
      DATEADD(DAY, -150, CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ))
    )
    AND receiver_type = 1
)
SELECT
  ANY_VALUE(merchants.name) AS merchant_name,
  COUNT(_s1.receiver_id) AS total_transactions,
  COALESCE(SUM(_s1.amount), 0) AS total_amount
FROM ewallet.merchants AS merchants
LEFT JOIN _s1 AS _s1
  ON _s1.receiver_id = merchants.mid
GROUP BY
  merchants.mid
ORDER BY
  3 DESC NULLS LAST
LIMIT 2
