SELECT
  agg_0 AS num_transactions,
  CASE WHEN agg_0 > 0 THEN COALESCE(agg_1, 0) ELSE NULL END AS total_amount
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
        sender_id
      FROM (
        SELECT
          amount,
          created_at,
          sender_id
        FROM main.wallet_transactions_daily
      ) AS _t2
      WHERE
        CAST((JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(created_at, 'start of day'))) AS INTEGER) <= 7
    ) AS _table_alias_0
    INNER JOIN (
      SELECT
        uid
      FROM (
        SELECT
          country,
          uid
        FROM main.users
      ) AS _t3
      WHERE
        country = 'US'
    ) AS _table_alias_1
      ON sender_id = uid
  ) AS _t1
) AS _t0
