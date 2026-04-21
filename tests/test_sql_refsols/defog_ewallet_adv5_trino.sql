SELECT
  AVG(balance) AS AMB
FROM mongo.defog.wallet_user_balance_daily
WHERE
  DATE_DIFF(
    'DAY',
    CAST(DATE_TRUNC('DAY', CAST(updated_at AS TIMESTAMP)) AS TIMESTAMP),
    CAST(DATE_TRUNC('DAY', CURRENT_TIMESTAMP) AS TIMESTAMP)
  ) <= 7
