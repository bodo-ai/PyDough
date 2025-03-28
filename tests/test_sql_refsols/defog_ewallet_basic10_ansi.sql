SELECT
  merchant_name,
  total_transactions,
  total_amount
FROM (
  SELECT
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
            created_at >= DATE_TRUNC('DAY', DATE_ADD(CURRENT_TIMESTAMP(), -150, 'DAY'))
          )
      )
      GROUP BY
        receiver_id
    )
      ON mid = receiver_id
  )
)
ORDER BY
  total_amount DESC
LIMIT 2
