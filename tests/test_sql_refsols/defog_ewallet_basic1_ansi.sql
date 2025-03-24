SELECT
  month,
  COUNT(DISTINCT sender_id) AS active_users
FROM (
  SELECT
    DATE_TRUNC('MONTH', CAST(created_at AS TIMESTAMP)) AS month,
    sender_id
  FROM (
    SELECT
      created_at,
      sender_id
    FROM (
      SELECT
        created_at,
        sender_id
      FROM (
        SELECT
          created_at,
          sender_id,
          sender_type,
          status
        FROM main.wallet_transactions_daily
      )
      WHERE
        (
          EXTRACT(YEAR FROM created_at) = 2023
        )
        AND (
          sender_type = 0
        )
        AND (
          status = 'success'
        )
    )
    INNER JOIN (
      SELECT
        uid
      FROM (
        SELECT
          status,
          uid
        FROM main.users
      )
      WHERE
        status = 'active'
    )
      ON sender_id = uid
  )
)
GROUP BY
  month
