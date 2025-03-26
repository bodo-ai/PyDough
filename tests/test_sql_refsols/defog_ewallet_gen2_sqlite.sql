WITH _table_alias_0 AS (
  SELECT
    MIN(user_setting_snapshot.snapshot_date) AS min_date
  FROM main.user_setting_snapshot AS user_setting_snapshot
  WHERE
    CAST(STRFTIME('%Y', user_setting_snapshot.snapshot_date) AS INTEGER) = 2023
), _table_alias_1 AS (
  SELECT
    user_setting_snapshot.snapshot_date AS snapshot_date,
    user_setting_snapshot.tx_limit_daily AS tx_limit_daily,
    user_setting_snapshot.tx_limit_monthly AS tx_limit_monthly
  FROM main.user_setting_snapshot AS user_setting_snapshot
  WHERE
    CAST(STRFTIME('%Y', user_setting_snapshot.snapshot_date) AS INTEGER) = 2023
)
SELECT
  AVG(_table_alias_1.tx_limit_daily) AS avg_daily_limit,
  AVG(_table_alias_1.tx_limit_monthly) AS avg_monthly_limit
FROM _table_alias_0 AS _table_alias_0
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0.min_date = _table_alias_1.snapshot_date
