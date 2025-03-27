SELECT
  quarter,
  customer_state,
  total_sales
FROM (
  SELECT
    customer_state AS ordering_2,
    quarter AS ordering_1,
    customer_state,
    quarter,
    total_sales
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS total_sales,
      customer_state,
      quarter
    FROM (
      SELECT
        SUM(sale_price) AS agg_0,
        customer_state,
        quarter
      FROM (
        SELECT
          state AS customer_state,
          IIF(
            CAST(STRFTIME('%m', sale_date) AS INTEGER) <= 3,
            '2023-01-01',
            IIF(
              CAST(STRFTIME('%m', sale_date) AS INTEGER) <= 6,
              '2023-04-01',
              IIF(CAST(STRFTIME('%m', sale_date) AS INTEGER) <= 9, '2023-07-01', '2023-10-01')
            )
          ) AS quarter,
          sale_price
        FROM (
          SELECT
            sale_date,
            sale_price,
            state
          FROM (
            SELECT
              customer_id,
              sale_date,
              sale_price
            FROM main.sales
            WHERE
              CAST(STRFTIME('%Y', sale_date) AS INTEGER) = 2023
          )
          LEFT JOIN (
            SELECT
              _id,
              state
            FROM main.customers
          )
            ON customer_id = _id
        )
      )
      GROUP BY
        customer_state,
        quarter
    )
  )
  WHERE
    total_sales > 0
)
ORDER BY
  ordering_1,
  ordering_2
