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
          ) AS _table_alias_0
          INNER JOIN (
            SELECT
              sbTxCustId AS customer_id,
              sbTxTickerId AS ticker_id
            FROM main.sbTransaction
          ) AS _table_alias_1
            ON _id = customer_id
        ) AS _table_alias_2
        INNER JOIN (
          SELECT
            sbTickerId AS _id,
            sbTickerType AS ticker_type
          FROM main.sbTicker
        ) AS _table_alias_3
          ON ticker_id = _id
      ) AS _t3
      GROUP BY
        state,
        ticker_type
    ) AS _t2
  ) AS _t1
  ORDER BY
    ordering_1 DESC
  LIMIT 5
) AS _t0
ORDER BY
  ordering_1 DESC
