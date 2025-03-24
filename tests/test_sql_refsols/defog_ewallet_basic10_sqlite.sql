SELECT
  merchant_name,
  total_transactions,
  total_amount
FROM (
  SELECT
    merchant_name,
    ordering_2,
    total_amount,
    total_transactions
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS ordering_2,
      COALESCE(agg_0, 0) AS total_amount,
      COALESCE(agg_1, 0) AS total_transactions,
      name AS merchant_name
    FROM (
      SELECT
        agg_0,
        agg_1,
        name
      FROM (
        SELECT
          mid,
          name
        FROM main.merchants
      )
      LEFT JOIN (
        SELECT
          COUNT() AS agg_1,
          SUM(amount) AS agg_0,
          receiver_id
        FROM (
          SELECT
            amount,
            receiver_id
          FROM (
            SELECT
              amount,
              created_at,
              receiver_id,
              receiver_type
            FROM main.wallet_transactions_daily
          )
          WHERE
            (
              receiver_type = 1
            )
            AND (
              created_at >= DATE(DATETIME('now', '-150 day'), 'start of day')
            )
        )
        GROUP BY
          receiver_id
      )
        ON mid = receiver_id
    )
  )
  ORDER BY
    ordering_2 DESC
  LIMIT 2
)
ORDER BY
  ordering_2 DESC
