WITH _t2 AS (
  SELECT
    COUNT() AS agg_0,
    status AS status
  FROM main.wallet_transactions_daily
  GROUP BY
    status
), _t0 AS (
  SELECT
    COALESCE(agg_0, 0) AS count,
    COALESCE(agg_0, 0) AS ordering_1,
    status AS status
  FROM _t2
  ORDER BY
    ordering_1 DESC
  LIMIT 3
)
SELECT
  status AS status,
  count AS count
FROM _t0
ORDER BY
  ordering_1 DESC
