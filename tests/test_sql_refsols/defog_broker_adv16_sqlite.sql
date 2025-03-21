SELECT
  symbol,
  SPM
FROM (
  SELECT
    symbol AS ordering_3,
    SPM,
    symbol
  FROM (
    SELECT
      CAST((
        100.0 * (
          COALESCE(agg_0, 0) - COALESCE(agg_1, 0)
        )
      ) AS REAL) / COALESCE(agg_2, 0) AS SPM,
      symbol
    FROM (
      SELECT
        agg_0,
        agg_1,
        agg_2,
        symbol
      FROM (
        SELECT
          sbTickerId AS _id,
          sbTickerSymbol AS symbol
        FROM main.sbTicker
      )
      LEFT JOIN (
        SELECT
          SUM(tax + commission) AS agg_1,
          SUM(amount) AS agg_0,
          SUM(amount) AS agg_2,
          ticker_id
        FROM (
          SELECT
            amount,
            commission,
            tax,
            ticker_id
          FROM (
            SELECT
              sbTxAmount AS amount,
              sbTxCommission AS commission,
              sbTxDateTime AS date_time,
              sbTxTax AS tax,
              sbTxTickerId AS ticker_id,
              sbTxType AS transaction_type
            FROM main.sbTransaction
          )
          WHERE
            (
              transaction_type = 'sell'
            )
            AND (
              date_time >= DATETIME('now', '-1 month')
            )
        )
        GROUP BY
          ticker_id
      )
        ON _id = ticker_id
    )
  )
  WHERE
    NOT (
      SPM IS NULL
    )
)
ORDER BY
  ordering_3
