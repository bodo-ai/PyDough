SELECT
  year_month,
  COUNT(DISTINCT sender_id) AS active_users
FROM (
  SELECT
    DATE_TRUNC('MONTH', CAST(created_at AS TIMESTAMP)) AS year_month,
    sender_id
  FROM (
    SELECT
      created_at,
      sender_id
    FROM (
      SELECT
        created_at,
        sender_id,
        sender_type
      FROM main.wallet_transactions_daily
    )
    WHERE
      (
        created_at < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
      )
      AND (
        sender_type = 0
      )
      AND (
        created_at >= DATE_ADD(DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()), -2, 'MONTH')
      )
  )
)
GROUP BY
  year_month
