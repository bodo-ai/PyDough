SELECT
  L_SHIPMODE,
  HIGH_LINE_COUNT,
  LOW_LINE_COUNT
FROM (
  SELECT
    COALESCE(agg_0, 0) AS HIGH_LINE_COUNT,
    COALESCE(agg_1, 0) AS LOW_LINE_COUNT,
    ship_mode AS L_SHIPMODE,
    ship_mode AS ordering_2
  FROM (
    SELECT
      SUM(expr_3) AS agg_1,
      SUM(is_high_priority) AS agg_0,
      ship_mode
    FROM (
      SELECT
        NOT is_high_priority AS expr_3,
        is_high_priority,
        ship_mode
      FROM (
        SELECT
          (
            order_priority = '1-URGENT'
          ) OR (
            order_priority = '2-HIGH'
          ) AS is_high_priority,
          ship_mode
        FROM (
          SELECT
            order_priority,
            ship_mode
          FROM (
            SELECT
              order_key,
              ship_mode
            FROM (
              SELECT
                l_commitdate AS commit_date,
                l_orderkey AS order_key,
                l_receiptdate AS receipt_date,
                l_shipdate AS ship_date,
                l_shipmode AS ship_mode
              FROM tpch.LINEITEM
            )
            WHERE
              (
                commit_date < receipt_date
              )
              AND (
                receipt_date < CAST('1995-01-01' AS DATE)
              )
              AND (
                ship_date < commit_date
              )
              AND (
                receipt_date >= CAST('1994-01-01' AS DATE)
              )
              AND (
                (
                  ship_mode = 'MAIL'
                ) OR (
                  ship_mode = 'SHIP'
                )
              )
          )
          LEFT JOIN (
            SELECT
              o_orderkey AS key,
              o_orderpriority AS order_priority
            FROM tpch.ORDERS
          )
            ON order_key = key
        )
      )
    )
    GROUP BY
      ship_mode
  )
)
ORDER BY
  ordering_2
