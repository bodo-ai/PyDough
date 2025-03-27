SELECT
  AVG(expr_1) AS _expr0
FROM (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY balance DESC) - 1.0
        ) - (
          CAST((
            COUNT(balance) OVER () - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN balance
      ELSE NULL
    END AS expr_1
  FROM (
    SELECT
      balance
    FROM (
      SELECT
        *
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY mid ORDER BY updated_at DESC) AS _w
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
      ) AS _t
      WHERE
        (
          DATE(updated_at, 'start of day') = DATE('now', 'start of day')
        )
        AND (
          _w = 1
        )
    )
  )
)
