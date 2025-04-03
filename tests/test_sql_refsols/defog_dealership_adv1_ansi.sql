SELECT
  payment_week,
  COALESCE(agg_0, 0) AS total_payments,
  COALESCE(agg_1, 0) AS weekend_payments
FROM (
  SELECT
    _table_alias_0.payment_week AS payment_week,
    agg_0,
    agg_1
  FROM (
    SELECT
      COUNT() AS agg_0,
      payment_week
    FROM (
      SELECT
        DATE_TRUNC('WEEK', CAST(payment_date AS TIMESTAMP)) AS payment_week
      FROM (
        SELECT
          payment_date
        FROM (
          SELECT
            payment_date,
            sale_id
          FROM main.payments_received
          WHERE
            (
              DATEDIFF(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), payment_date, WEEK) <= 8
            )
            AND (
              DATEDIFF(CURRENT_TIMESTAMP(), payment_date, WEEK) >= 1
            )
        )
        INNER JOIN (
          SELECT
            _id
          FROM (
            SELECT
              _id,
              sale_price
            FROM main.sales
          )
          WHERE
            sale_price > 30000
        )
          ON sale_id = _id
      )
    )
    GROUP BY
      payment_week
  ) AS _table_alias_0
  LEFT JOIN (
    SELECT
      COUNT() AS agg_1,
      payment_week
    FROM (
      SELECT
        DATE_TRUNC('WEEK', CAST(payment_date AS TIMESTAMP)) AS payment_week
      FROM (
        SELECT
          payment_date
        FROM (
          SELECT
            payment_date,
            sale_id
          FROM main.payments_received
          WHERE
            (
              DATEDIFF(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), payment_date, WEEK) <= 8
            )
            AND (
              DATEDIFF(CURRENT_TIMESTAMP(), payment_date, WEEK) >= 1
            )
            AND (
              (
                (
                  (
                    (
                      DAY_OF_WEEK(payment_date) + 6
                    ) % 7
                  )
                ) = 6
              )
              OR (
                (
                  (
                    (
                      DAY_OF_WEEK(payment_date) + 6
                    ) % 7
                  )
                ) = 7
              )
            )
        )
        INNER JOIN (
          SELECT
            _id
          FROM (
            SELECT
              _id,
              sale_price
            FROM main.sales
          )
          WHERE
            sale_price > 30000
        )
          ON sale_id = _id
      )
    )
    GROUP BY
      payment_week
  ) AS _table_alias_1
    ON _table_alias_0.payment_week = _table_alias_1.payment_week
)
