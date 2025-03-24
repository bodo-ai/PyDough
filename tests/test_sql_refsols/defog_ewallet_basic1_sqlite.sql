SELECT
  month,
  COUNT(DISTINCT sender_id) AS active_users
FROM (
  SELECT
    DATE(created_at, 'start of month') AS month,
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
          CAST(STRFTIME('%Y', created_at) AS INTEGER) = 2023
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
