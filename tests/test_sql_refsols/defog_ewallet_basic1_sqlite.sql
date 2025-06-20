SELECT
  DATE(_s0.created_at, 'start of month') AS month,
  COUNT(DISTINCT _s0.sender_id) AS active_users
FROM main.wallet_transactions_daily AS _s0
JOIN main.users AS _s1
  ON _s0.sender_id = _s1.uid AND _s1.status = 'active'
WHERE
  CAST(STRFTIME('%Y', _s0.created_at) AS INTEGER) = 2023
  AND _s0.sender_type = 0
  AND _s0.status = 'success'
GROUP BY
  DATE(_s0.created_at, 'start of month')
