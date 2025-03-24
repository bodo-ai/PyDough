SELECT
  AVG(tx_limit_daily) AS avg_daily_limit,
  AVG(tx_limit_monthly) AS avg_monthly_limit
FROM (
  SELECT
    tx_limit_daily,
    tx_limit_monthly
  FROM (
    SELECT
      min_date,
      snapshot_date,
      tx_limit_daily,
      tx_limit_monthly
    FROM (
      SELECT
        MIN(snapshot_date) AS min_date
      FROM (
        SELECT
          snapshot_date
        FROM main.user_setting_snapshot
        WHERE
          EXTRACT(YEAR FROM snapshot_date) = 2023
      )
    )
    INNER JOIN (
      SELECT
        snapshot_date,
        tx_limit_daily,
        tx_limit_monthly
      FROM main.user_setting_snapshot
      WHERE
        EXTRACT(YEAR FROM snapshot_date) = 2023
    )
      ON TRUE
  )
  WHERE
    min_date = snapshot_date
)
