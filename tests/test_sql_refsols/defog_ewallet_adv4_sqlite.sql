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
      FROM main.wallet_transactions_daily
      WHERE
        CAST((JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(created_at, 'start of day'))) AS INTEGER) <= 7
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
