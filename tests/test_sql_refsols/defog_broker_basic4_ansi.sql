SELECT
  state,
  ticker_type,
  num_transactions
FROM (
  SELECT
    COALESCE(agg_0, 0) AS num_transactions,
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
      INNER JOIN (
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
  num_transactions DESC
LIMIT 5
