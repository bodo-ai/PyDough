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
      SUM(is_high_priority) AS agg_0,
      SUM(NOT is_high_priority) AS agg_1,
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
          ) AS _t4
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
        ) AS _table_alias_0
        LEFT JOIN (
          SELECT
            o_orderkey AS key,
            o_orderpriority AS order_priority
          FROM tpch.ORDERS
        ) AS _table_alias_1
          ON order_key = key
      ) AS _t3
    ) AS _t2
    GROUP BY
      ship_mode
  ) AS _t1
) AS _t0
ORDER BY
  ordering_2
