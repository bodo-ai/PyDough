WITH _s3 AS (
  SELECT
    COUNT(*) AS total_transactions,
    sender_id
  FROM main.wallet_transactions_daily
  WHERE
    sender_type = 0
  GROUP BY
    sender_id
)
SELECT
  _s0.uid AS user_id,
  _s3.total_transactions
FROM main.users AS _s0
JOIN _s3 AS _s3
  ON _s0.uid = _s3.sender_id
