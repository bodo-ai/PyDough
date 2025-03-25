SELECT
  symbol,
  tx_count
FROM (
  SELECT
    ordering_1,
    symbol,
    tx_count
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS ordering_1,
      COALESCE(agg_0, 0) AS tx_count,
      symbol
    FROM (
      SELECT
        agg_0,
        symbol
      FROM (
        SELECT
          sbTickerId AS _id,
          sbTickerSymbol AS symbol
        FROM main.sbTicker
      ) AS _table_alias_0
      LEFT JOIN (
        SELECT
          COUNT() AS agg_0,
          ticker_id
        FROM (
          SELECT
            ticker_id
          FROM (
            SELECT
              sbTxDateTime AS date_time,
              sbTxTickerId AS ticker_id,
              sbTxType AS transaction_type
            FROM main.sbTransaction
          ) AS _t4
          WHERE
            (
              transaction_type = 'buy'
            )
            AND (
              date_time >= DATE(DATETIME('now', '-10 day'), 'start of day')
            )
        ) AS _t3
        GROUP BY
          ticker_id
      ) AS _table_alias_1
        ON _id = ticker_id
    ) AS _t2
  ) AS _t1
  ORDER BY
    ordering_1 DESC
  LIMIT 2
) AS _t0
ORDER BY
  ordering_1 DESC
