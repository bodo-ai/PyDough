SELECT
  state,
  unique_customers,
  total_revenue
FROM (
  SELECT
    ordering_2,
    state,
    total_revenue,
    unique_customers
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS ordering_2,
      COALESCE(agg_0, 0) AS total_revenue,
      agg_1 AS unique_customers,
      state
    FROM (
      SELECT
        _table_alias_2.state AS state,
        agg_0,
        agg_1
      FROM (
        SELECT
          _table_alias_0.state AS state,
          agg_1
        FROM (
          SELECT DISTINCT
            state
          FROM (
            SELECT
              _id,
              state
            FROM main.customers
          )
          WHERE
            EXISTS(
              SELECT
                1
              FROM (
                SELECT
                  customer_id
                FROM main.sales
              )
              WHERE
                _id = customer_id
            )
        ) AS _table_alias_0
        LEFT JOIN (
          SELECT
            COUNT(DISTINCT _id) AS agg_1,
            state
          FROM (
            SELECT
              _id,
              state
            FROM (
              SELECT
                _id,
                state
              FROM main.customers
              WHERE
                _id <> 'NULL'
            )
            WHERE
              EXISTS(
                SELECT
                  1
                FROM (
                  SELECT
                    customer_id
                  FROM main.sales
                )
                WHERE
                  _id = customer_id
              )
          )
          GROUP BY
            state
        ) AS _table_alias_1
          ON _table_alias_0.state = _table_alias_1.state
      ) AS _table_alias_2
      LEFT JOIN (
        SELECT
          SUM(sale_price) AS agg_0,
          state
        FROM (
          SELECT
            sale_price,
            state
          FROM (
            SELECT
              _id,
              state
            FROM (
              SELECT
                _id,
                state
              FROM main.customers
            )
            WHERE
              EXISTS(
                SELECT
                  1
                FROM (
                  SELECT
                    customer_id
                  FROM main.sales
                )
                WHERE
                  _id = customer_id
              )
          )
          INNER JOIN (
            SELECT
              customer_id,
              sale_price
            FROM main.sales
          )
            ON _id = customer_id
        )
        GROUP BY
          state
      ) AS _table_alias_3
        ON _table_alias_2.state = _table_alias_3.state
    )
  )
  ORDER BY
    ordering_2 DESC
  LIMIT 5
)
ORDER BY
  ordering_2 DESC
