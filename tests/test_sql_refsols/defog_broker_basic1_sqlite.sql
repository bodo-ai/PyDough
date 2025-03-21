SELECT
  country,
  COALESCE(agg_0, 0) AS num_transactions,
  COALESCE(agg_1, 0) AS total_amount
FROM (
  SELECT
    _table_alias_0.country AS country,
    agg_0,
    agg_1
  FROM (
    SELECT DISTINCT
      country
    FROM (
      SELECT
        sbCustCountry AS country
      FROM main.sbCustomer
    )
  ) AS _table_alias_0
  LEFT JOIN (
    SELECT
      COUNT() AS agg_0,
      SUM(amount) AS agg_1,
      country
    FROM (
      SELECT
        amount,
        country
      FROM (
        SELECT
          sbCustId AS _id,
          sbCustCountry AS country
        FROM main.sbCustomer
      )
      INNER JOIN (
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
          date_time >= DATE(DATETIME('now', '-30 day'), 'start of day')
      )
        ON _id = customer_id
    )
    GROUP BY
      country
  ) AS _table_alias_1
    ON _table_alias_0.country = _table_alias_1.country
)
