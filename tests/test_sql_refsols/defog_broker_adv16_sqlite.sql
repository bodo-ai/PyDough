SELECT
  symbol,
  SPM
FROM (
  SELECT
    symbol AS ordering_2,
    SPM,
    symbol
  FROM (
    SELECT
      CAST((
        100.0 * (
          COALESCE(agg_0, 0) - COALESCE(agg_1, 0)
        )
      ) AS REAL) / COALESCE(agg_0, 0) AS SPM,
      symbol
    FROM (
      SELECT
        agg_0,
        agg_1,
        symbol
      FROM (
        SELECT
          sbTickerId AS _id,
          sbTickerSymbol AS symbol
        FROM main.sbTicker
      ) AS _table_alias_0
      LEFT JOIN (
        SELECT
          SUM(tax + commission) AS agg_1,
          SUM(amount) AS agg_0,
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
          ) AS _t4
          WHERE
            (
              transaction_type = 'sell'
            )
            AND (
              date_time >= DATETIME('now', '-1 month')
            )
        ) AS _t3
        GROUP BY
          ticker_id
      ) AS _table_alias_1
        ON _id = ticker_id
    ) AS _t2
  ) AS _t1
  WHERE
    NOT (
      SPM IS NULL
    )
) AS _t0
ORDER BY
  ordering_2
