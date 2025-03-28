SELECT
  year_month,
  COUNT(DISTINCT sender_id) AS active_users
FROM (
  SELECT
    DATE(created_at, 'start of month') AS year_month,
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
        created_at < DATE('now', 'start of month')
      )
      AND (
        sender_type = 0
      )
      AND (
        created_at >= DATE('now', 'start of month', '-2 month')
      )
  )
)
GROUP BY
  year_month
