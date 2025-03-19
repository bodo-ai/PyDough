SELECT
  COALESCE(agg_0, 0) AS n
FROM (
  SELECT
    SUM(n_above_avg) AS agg_0
  FROM (
    SELECT
      COALESCE(agg_2, 0) AS n_above_avg
    FROM (
      SELECT
        COUNT() AS agg_2,
        customer_key,
        order_date
      FROM (
        SELECT
          customer_key,
          order_date
        FROM (
          SELECT
            _table_alias_0.customer_key AS customer_key,
            _table_alias_0.order_date AS order_date,
            total_price,
            total_price_sum
          FROM (
            SELECT
              COALESCE(agg_1, 0) AS total_price_sum,
              customer_key,
              order_date
            FROM (
              SELECT
                agg_1,
                customer_key,
                order_date
              FROM (
                SELECT
                  COUNT() AS agg_0,
                  SUM(total_price) AS agg_1,
                  customer_key,
                  order_date
                FROM (
                  SELECT
                    customer_key,
                    order_date,
                    total_price
                  FROM (
                    SELECT
                      o_custkey AS customer_key,
                      o_orderdate AS order_date,
                      o_totalprice AS total_price
                    FROM tpch.ORDERS
                  )
                  WHERE
                    YEAR(order_date) = 1993
                )
                GROUP BY
                  order_date,
                  customer_key
              )
              WHERE
                COALESCE(agg_0, 0) > 1
            )
          ) AS _table_alias_0
          INNER JOIN (
            SELECT
              customer_key,
              order_date,
              total_price
            FROM (
              SELECT
                o_custkey AS customer_key,
                o_orderdate AS order_date,
                o_totalprice AS total_price
              FROM tpch.ORDERS
            )
            WHERE
              YEAR(order_date) = 1993
          ) AS _table_alias_1
            ON (
              _table_alias_0.customer_key = _table_alias_1.customer_key
            )
            AND (
              _table_alias_0.order_date = _table_alias_1.order_date
            )
        )
        WHERE
          total_price >= (
            0.5 * total_price_sum
          )
      )
      GROUP BY
        order_date,
        customer_key
    )
  )
)
