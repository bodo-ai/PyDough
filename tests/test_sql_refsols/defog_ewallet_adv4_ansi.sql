SELECT
  agg_0 AS num_transactions,
  CASE WHEN agg_0 > 0 THEN COALESCE(agg_1, 0) ELSE NULL END AS total_amount
FROM (
  SELECT
    COUNT() AS agg_0,
    SUM(amount) AS agg_1
  FROM (
    SELECT
      amount
    FROM (
      SELECT
        amount,
        sender_id
      FROM (
        SELECT
          amount,
          created_at,
          sender_id
        FROM main.wallet_transactions_daily
      )
      WHERE
        DATEDIFF(CURRENT_TIMESTAMP(), created_at, DAY) <= 7
    )
    INNER JOIN (
      SELECT
        uid
      FROM (
        SELECT
          country,
          uid
        FROM main.users
      )
      WHERE
        country = 'US'
    )
      ON sender_id = uid
  )
)
