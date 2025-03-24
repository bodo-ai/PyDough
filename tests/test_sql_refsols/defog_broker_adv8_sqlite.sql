SELECT
  CASE WHEN agg_0 > 0 THEN agg_0 ELSE NULL END AS n_transactions,
  COALESCE(agg_1, 0) AS total_amount
FROM (
  SELECT
    COUNT() AS agg_0,
    SUM(amount) AS agg_1
  FROM (
    SELECT
      amount
    FROM (
      SELECT
        amount,
        customer_id
      FROM (
        SELECT
          sbTxAmount AS amount,
          sbTxCustId AS customer_id,
          sbTxDateTime AS date_time
        FROM main.sbTransaction
      )
      WHERE
        (
          date_time < DATE(
            'now',
            '-' || CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) || ' days',
            'start of day'
          )
        )
        AND (
          date_time >= DATE(
            'now',
            '-' || CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) || ' days',
            'start of day',
            '-7 day'
          )
        )
    )
    WHERE
      EXISTS(
        SELECT
          1
        FROM (
          SELECT
            _id
          FROM (
            SELECT
              sbCustCountry AS country,
              sbCustId AS _id
            FROM main.sbCustomer
          )
          WHERE
            LOWER(country) = 'usa'
        )
        WHERE
          customer_id = _id
      )
  )
)
