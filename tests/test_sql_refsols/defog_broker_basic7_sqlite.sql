WITH _t2 AS (
  SELECT
    COUNT() AS agg_0,
    sbtxstatus AS status
  FROM main.sbtransaction
  GROUP BY
    sbtxstatus
), _t0 AS (
  SELECT
    COALESCE(agg_0, 0) AS num_transactions,
    COALESCE(agg_0, 0) AS ordering_1,
    status
  FROM _t2
  ORDER BY
    ordering_1 DESC
  LIMIT 3
)
SELECT
  status,
  num_transactions
FROM _t0
ORDER BY
  ordering_1 DESC
