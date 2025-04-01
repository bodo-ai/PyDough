SELECT
  state,
  ticker_type,
  num_transactions
FROM (
  SELECT
    num_transactions,
    ordering_1,
    state,
    ticker_type
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS num_transactions,
      COALESCE(agg_0, 0) AS ordering_1,
      state,
      ticker_type
    FROM (
      SELECT
        COUNT() AS agg_0,
        state,
        ticker_type
      FROM (
        SELECT
          state,
          ticker_type
        FROM (
          SELECT
            state,
            ticker_id
          FROM (
            SELECT
              sbCustId AS _id,
              sbCustState AS state
            FROM main.sbCustomer
          )
          INNER JOIN (
            SELECT
              sbTxCustId AS customer_id,
              sbTxTickerId AS ticker_id
            FROM main.sbTransaction
          )
            ON _id = customer_id
        )
        LEFT JOIN (
          SELECT
            sbTickerId AS _id,
            sbTickerType AS ticker_type
          FROM main.sbTicker
        )
          ON ticker_id = _id
      )
      GROUP BY
        state,
        ticker_type
    )
  )
  ORDER BY
    ordering_1 DESC
  LIMIT 5
)
ORDER BY
  ordering_1 DESC
