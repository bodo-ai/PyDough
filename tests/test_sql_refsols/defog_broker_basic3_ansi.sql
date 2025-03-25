SELECT
  symbol,
  num_transactions,
  total_amount
FROM (
  SELECT
    num_transactions,
    ordering_2,
    symbol,
    total_amount
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS num_transactions,
      COALESCE(agg_1, 0) AS ordering_2,
      COALESCE(agg_1, 0) AS total_amount,
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
          COUNT() AS agg_0,
          SUM(amount) AS agg_1,
          ticker_id
        FROM (
          SELECT
            sbTxAmount AS amount,
            sbTxTickerId AS ticker_id
          FROM main.sbTransaction
        ) AS _t3
        GROUP BY
          ticker_id
      ) AS _table_alias_1
        ON _id = ticker_id
    ) AS _t2
  ) AS _t1
  ORDER BY
    ordering_2 DESC
  LIMIT 10
) AS _t0
ORDER BY
  ordering_2 DESC
