SELECT
  country,
  user_count,
  total_amount
FROM (
  SELECT
    COALESCE(agg_0, 0) AS total_amount,
    agg_1 AS user_count,
    country
  FROM (
    SELECT
      COUNT(DISTINCT sender_id) AS agg_1,
      SUM(amount) AS agg_0,
      country
    FROM (
      SELECT
        amount,
        country,
        sender_id
      FROM (
        SELECT
          amount,
          sender_id
        FROM (
          SELECT
            amount,
            sender_id,
            sender_type
          FROM main.wallet_transactions_daily
        )
        WHERE
          sender_type = 0
      )
      INNER JOIN (
        SELECT
          country,
          uid
        FROM main.users
      )
        ON sender_id = uid
    )
    GROUP BY
      country
  )
)
ORDER BY
  total_amount DESC
LIMIT 5
