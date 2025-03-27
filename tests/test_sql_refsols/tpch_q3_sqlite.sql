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
            ) AS _t4
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
            ) AS _t5
            WHERE
              mktsegment = 'BUILDING'
          ) AS _table_alias_1
            ON customer_key = _table_alias_1.key
        ) AS _table_alias_2
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
          ) AS _t6
          WHERE
            ship_date > '1995-03-15'
        ) AS _table_alias_3
          ON key = order_key
      ) AS _t3
      GROUP BY
        order_date,
        order_key,
        ship_priority
    ) AS _t2
  ) AS _t1
  ORDER BY
    ordering_1 DESC,
    ordering_2,
    ordering_3
  LIMIT 10
) AS _t0
ORDER BY
  ordering_1 DESC,
  ordering_2,
  ordering_3
