WITH _s2 AS (
  SELECT
    MIN(snapshot_date) AS min_date
  FROM main.user_setting_snapshot
  WHERE
    CAST(STRFTIME('%Y', snapshot_date) AS INTEGER) = 2023
)
SELECT
  AVG(_s1.tx_limit_daily) AS avg_daily_limit,
  AVG(_s1.tx_limit_monthly) AS avg_monthly_limit
FROM _s2 AS _s2
JOIN main.user_setting_snapshot AS _s1
  ON CAST(STRFTIME('%Y', _s1.snapshot_date) AS INTEGER) = 2023
  AND _s1.snapshot_date = _s2.min_date
