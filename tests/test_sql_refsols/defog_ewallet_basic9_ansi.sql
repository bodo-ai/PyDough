WITH _s3 AS (
  SELECT
    country,
    uid
  FROM main.users
), _t1 AS (
  SELECT
    SUM(_s0.amount) AS agg_0,
    COUNT(DISTINCT _s0.sender_id) AS agg_1,
    _s3.country
  FROM main.wallet_transactions_daily AS _s0
  LEFT JOIN _s3 AS _s3
    ON _s0.sender_id = _s3.uid
  WHERE
    _s0.sender_type = 0
  GROUP BY
    _s3.country
)
SELECT
  country,
  agg_1 AS user_count,
  COALESCE(agg_0, 0) AS total_amount
FROM _t1
ORDER BY
  total_amount DESC
LIMIT 5
