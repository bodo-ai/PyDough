WITH _s0 AS (
  SELECT
    MIN(snapshot_date) AS min_snapshotdate
  FROM main.user_setting_snapshot
  WHERE
    EXTRACT(YEAR FROM CAST(snapshot_date AS TIMESTAMP)) = 2023
), _s1 AS (
  SELECT
    snapshot_date,
    AVG(CAST(tx_limit_daily AS DECIMAL)) AS avg_txlimitdaily,
    AVG(CAST(tx_limit_monthly AS DECIMAL)) AS avg_txlimitmonthly
  FROM main.user_setting_snapshot
  WHERE
    EXTRACT(YEAR FROM CAST(snapshot_date AS TIMESTAMP)) = 2023
  GROUP BY
    1
)
SELECT
  _s1.avg_txlimitdaily AS avg_daily_limit,
  _s1.avg_txlimitmonthly AS avg_monthly_limit
FROM _s0 AS _s0
LEFT JOIN _s1 AS _s1
  ON _s0.min_snapshotdate = _s1.snapshot_date
