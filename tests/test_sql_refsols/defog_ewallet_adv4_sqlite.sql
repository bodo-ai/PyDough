WITH _s0 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(amount) AS agg_1,
    sender_id
  FROM main.wallet_transactions_daily
  WHERE
    CAST((
      JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(created_at, 'start of day'))
    ) AS INTEGER) <= 7
  GROUP BY
    sender_id
), _t1 AS (
  SELECT
    SUM(_s0.agg_0) AS agg_0,
    SUM(_s0.agg_1) AS agg_1
  FROM _s0 AS _s0
  JOIN main.users AS users
    ON _s0.sender_id = users.uid AND users.country = 'US'
)
SELECT
  COALESCE(agg_0, 0) AS num_transactions,
  CASE
    WHEN (
      NOT agg_0 IS NULL AND agg_0 > 0
    )
    THEN COALESCE(agg_1, 0)
    ELSE NULL
  END AS total_amount
FROM _t1
