SELECT
  ROUND(agg_0, 2) AS avg_days_to_payment
FROM (
  SELECT
    AVG(sale_pay_diff) AS agg_0
  FROM (
    SELECT
      DATEDIFF(agg_0, sale_date, DAY) AS sale_pay_diff
    FROM (
      SELECT
        _table_alias_0.sale_date AS sale_date,
        agg_0
      FROM (
        SELECT DISTINCT
          _id,
          sale_date
        FROM main.sales
      ) AS _table_alias_0
      LEFT JOIN (
        SELECT
          MAX(payment_date) AS agg_0,
          _id,
          sale_date
        FROM (
          SELECT
            _id,
            payment_date,
            sale_date
          FROM (
            SELECT
              _id,
              sale_date
            FROM main.sales
          )
          INNER JOIN (
            SELECT
              payment_date,
              sale_id
            FROM main.payments_received
          )
            ON _id = sale_id
        )
        GROUP BY
          _id,
          sale_date
      ) AS _table_alias_1
        ON (
          _table_alias_0._id = _table_alias_1._id
        )
        AND (
          _table_alias_0.sale_date = _table_alias_1.sale_date
        )
    )
  )
)
