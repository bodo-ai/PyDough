SELECT
  O_ORDERPRIORITY,
  ORDER_COUNT
FROM (
  SELECT
    COALESCE(agg_0, 0) AS ORDER_COUNT,
    order_priority AS O_ORDERPRIORITY,
    order_priority AS ordering_1
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
        ) AS _t3
        WHERE
          (
            order_date < '1993-10-01'
          ) AND (
            order_date >= '1993-07-01'
          )
      ) AS _table_alias_0
      WHERE
        EXISTS(
          SELECT
            1
          FROM (
            SELECT
              order_key
            FROM (
              SELECT
                l_commitdate AS commit_date,
                l_orderkey AS order_key,
                l_receiptdate AS receipt_date
              FROM tpch.LINEITEM
            ) AS _t4
            WHERE
              commit_date < receipt_date
          ) AS _table_alias_1
          WHERE
            key = order_key
        )
    ) AS _t2
    GROUP BY
      order_priority
  ) AS _t1
) AS _t0
ORDER BY
  ordering_1
