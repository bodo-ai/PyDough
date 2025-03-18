SELECT
  year_7 AS year,
  month_6 AS month,
  n_orders_in_range
FROM (
  SELECT
    COALESCE(agg_2, 0) AS n_orders_in_range,
    agg_1 AS month_6,
    agg_1 AS ordering_4,
    agg_4 AS ordering_3,
    agg_4 AS year_7
  FROM (
    SELECT
      MAX(month) AS agg_1,
      MAX(year) AS agg_4,
      COUNT() AS agg_2,
      month,
      year
    FROM (
      SELECT
        month,
        year
      FROM (
        SELECT
          _table_alias_0.month AS month,
          _table_alias_0.year AS year,
          curr_month_avg_price,
          prev_month_avg_price,
          total_price
        FROM (
          SELECT
            agg_0 AS curr_month_avg_price,
            LAG(agg_1, 1) OVER (ORDER BY year, month) AS prev_month_avg_price,
            month,
            year
          FROM (
            SELECT
              AVG(total_price) AS agg_0,
              AVG(total_price) AS agg_1,
              month,
              year
            FROM (
              SELECT
                month,
                total_price,
                year
              FROM (
                SELECT
                  CAST(STRFTIME('%Y', order_date) AS INTEGER) AS year,
                  CAST(STRFTIME('%m', order_date) AS INTEGER) AS month,
                  total_price
                FROM (
                  SELECT
                    o_orderdate AS order_date,
                    o_totalprice AS total_price
                  FROM tpch.ORDERS
                )
              )
              WHERE
                year < 1994
            )
            GROUP BY
              month,
              year
          )
        ) AS _table_alias_0
        INNER JOIN (
          SELECT
            month,
            total_price,
            year
          FROM (
            SELECT
              CAST(STRFTIME('%Y', order_date) AS INTEGER) AS year,
              CAST(STRFTIME('%m', order_date) AS INTEGER) AS month,
              total_price
            FROM (
              SELECT
                o_orderdate AS order_date,
                o_totalprice AS total_price
              FROM tpch.ORDERS
            )
          )
          WHERE
            year < 1994
        ) AS _table_alias_1
          ON (
            _table_alias_0.month = _table_alias_1.month
          )
          AND (
            _table_alias_0.year = _table_alias_1.year
          )
      )
      WHERE
        (
          (
            prev_month_avg_price <= total_price
          )
          AND (
            total_price <= curr_month_avg_price
          )
        )
        OR (
          (
            curr_month_avg_price <= total_price
          )
          AND (
            total_price <= prev_month_avg_price
          )
        )
    )
    GROUP BY
      month,
      year
  )
)
ORDER BY
  ordering_3,
  ordering_4
