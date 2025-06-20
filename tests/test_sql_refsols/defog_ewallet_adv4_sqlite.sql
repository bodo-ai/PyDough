WITH _t0 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM(_s0.amount) AS agg_1
  FROM main.wallet_transactions_daily AS _s0
  JOIN main.users AS _s1
    ON _s0.sender_id = _s1.uid AND _s1.country = 'US'
  WHERE
    CAST((
      JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(_s0.created_at, 'start of day'))
    ) AS INTEGER) <= 7
)
SELECT
  agg_0 AS num_transactions,
  CASE WHEN agg_0 > 0 THEN COALESCE(agg_1, 0) ELSE NULL END AS total_amount
FROM _t0
