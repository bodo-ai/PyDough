SELECT
  uid AS user_id,
  COALESCE(agg_0, 0) AS total_transactions
FROM (
  SELECT
    agg_0,
    uid
  FROM (
    SELECT
      uid
    FROM main.users
  )
  INNER JOIN (
    SELECT
      COUNT() AS agg_0,
      sender_id
    FROM (
      SELECT
        sender_id
      FROM (
        SELECT
          sender_id,
          sender_type
        FROM main.wallet_transactions_daily
      )
      WHERE
        sender_type = 0
    )
    GROUP BY
      sender_id
  )
    ON uid = sender_id
)
