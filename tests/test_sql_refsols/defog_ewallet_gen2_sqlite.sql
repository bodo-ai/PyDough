WITH _table_alias_0 AS (
  SELECT
    MIN(user_setting_snapshot.snapshot_date) AS min_date
  FROM main.user_setting_snapshot AS user_setting_snapshot
  WHERE
    CAST(STRFTIME('%Y', user_setting_snapshot.snapshot_date) AS INTEGER) = 2023
)
SELECT
  AVG(user_setting_snapshot.tx_limit_daily) AS avg_daily_limit,
  AVG(user_setting_snapshot.tx_limit_monthly) AS avg_monthly_limit
FROM _table_alias_0 AS _table_alias_0
JOIN main.user_setting_snapshot AS user_setting_snapshot
  ON CAST(STRFTIME('%Y', user_setting_snapshot.snapshot_date) AS INTEGER) = 2023
  AND _table_alias_0.min_date = user_setting_snapshot.snapshot_date
