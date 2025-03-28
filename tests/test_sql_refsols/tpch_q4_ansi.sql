SELECT
  O_ORDERPRIORITY,
  ORDER_COUNT
FROM (
  SELECT
    COALESCE(agg_0, 0) AS ORDER_COUNT,
    order_priority AS O_ORDERPRIORITY
  FROM (
    SELECT
      COUNT() AS agg_0,
      order_priority
    FROM (
      SELECT
        order_priority
      FROM (
        SELECT
          key,
          order_priority
        FROM (
          SELECT
            o_orderdate AS order_date,
            o_orderkey AS key,
            o_orderpriority AS order_priority
          FROM tpch.ORDERS
        )
        WHERE
          (
            order_date < CAST('1993-10-01' AS DATE)
          )
          AND (
            order_date >= CAST('1993-07-01' AS DATE)
          )
      )
      SEMI JOIN (
        SELECT
          order_key
        FROM (
          SELECT
            l_commitdate AS commit_date,
            l_orderkey AS order_key,
            l_receiptdate AS receipt_date
          FROM tpch.LINEITEM
        )
        WHERE
          commit_date < receipt_date
      )
        ON key = order_key
    )
    GROUP BY
      order_priority
  )
)
ORDER BY
  O_ORDERPRIORITY
