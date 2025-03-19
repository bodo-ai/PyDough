SELECT
  L_ORDERKEY,
  REVENUE,
  O_ORDERDATE,
  O_SHIPPRIORITY
FROM (
  SELECT
    L_ORDERKEY,
    O_ORDERDATE,
    O_SHIPPRIORITY,
    REVENUE,
    ordering_1,
    ordering_2,
    ordering_3
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS REVENUE,
      COALESCE(agg_0, 0) AS ordering_1,
      order_date AS O_ORDERDATE,
      order_date AS ordering_2,
      order_key AS L_ORDERKEY,
      order_key AS ordering_3,
      ship_priority AS O_SHIPPRIORITY
    FROM (
      SELECT
        SUM(extended_price * (
          1 - discount
        )) AS agg_0,
        order_date,
        order_key,
        ship_priority
      FROM (
        SELECT
          discount,
          extended_price,
          order_date,
          order_key,
          ship_priority
        FROM (
          SELECT
            _table_alias_0.key AS key,
            order_date,
            ship_priority
          FROM (
            SELECT
              customer_key,
              key,
              order_date,
              ship_priority
            FROM (
              SELECT
                o_custkey AS customer_key,
                o_orderdate AS order_date,
                o_orderkey AS key,
                o_shippriority AS ship_priority
              FROM tpch.ORDERS
            )
            WHERE
              order_date < '1995-03-15'
          ) AS _table_alias_0
          INNER JOIN (
            SELECT
              key
            FROM (
              SELECT
                c_custkey AS key,
                c_mktsegment AS mktsegment
              FROM tpch.CUSTOMER
            )
            WHERE
              mktsegment = 'BUILDING'
          ) AS _table_alias_1
            ON customer_key = _table_alias_1.key
        )
        INNER JOIN (
          SELECT
            discount,
            extended_price,
            order_key
          FROM (
            SELECT
              l_discount AS discount,
              l_extendedprice AS extended_price,
              l_orderkey AS order_key,
              l_shipdate AS ship_date
            FROM tpch.LINEITEM
          )
          WHERE
            ship_date > '1995-03-15'
        )
          ON key = order_key
      )
      GROUP BY
        ship_priority,
        order_key,
        order_date
    )
  )
  ORDER BY
    ordering_1 DESC,
    ordering_2,
    ordering_3
  LIMIT 10
)
ORDER BY
  ordering_1 DESC,
  ordering_2,
  ordering_3
