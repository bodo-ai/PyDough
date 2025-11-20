WITH _s0 AS (
  SELECT
    MIN(snapshot_date) AS min_snapshot_date
  FROM main.user_setting_snapshot
  WHERE
    EXTRACT(YEAR FROM CAST(snapshot_date AS DATETIME)) = 2023
), _s1 AS (
  SELECT
    snapshot_date,
    AVG(tx_limit_daily) AS avg_tx_limit_daily,
    AVG(tx_limit_monthly) AS avg_tx_limit_monthly
  FROM main.user_setting_snapshot
  WHERE
    EXTRACT(YEAR FROM CAST(snapshot_date AS DATETIME)) = 2023
  GROUP BY
    1
)
SELECT
  _s1.avg_tx_limit_daily AS avg_daily_limit,
  _s1.avg_tx_limit_monthly AS avg_monthly_limit
FROM _s0 AS _s0
LEFT JOIN _s1 AS _s1
  ON _s0.min_snapshot_date = _s1.snapshot_date
