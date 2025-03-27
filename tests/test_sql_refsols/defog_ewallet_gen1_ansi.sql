SELECT
  MEDIAN(balance) AS _expr0
FROM (
  SELECT
    balance
  FROM (
    SELECT
      *
    FROM (
      SELECT
        balance,
        mid,
        updated_at
      FROM (
        SELECT
          mid
        FROM (
          SELECT
            category,
            mid,
            status
          FROM main.merchants
        )
        WHERE
          (
            status = 'active'
          ) AND (
            LOWER(category) LIKE '%retail%'
          )
      )
      INNER JOIN (
        SELECT
          balance,
          merchant_id,
          updated_at
        FROM main.wallet_merchant_balance_daily
      )
        ON mid = merchant_id
    )
    QUALIFY
      (
        DATE_TRUNC('DAY', CAST(updated_at AS TIMESTAMP)) = DATE_TRUNC('DAY', CURRENT_TIMESTAMP())
      )
      AND (
        ROW_NUMBER() OVER (PARTITION BY mid ORDER BY updated_at DESC NULLS FIRST) = 1
      )
  )
)
