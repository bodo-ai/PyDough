WITH _s0 AS (
  SELECT
    MIN(snapshot_date) AS min_date
  FROM main.user_setting_snapshot
  WHERE
    EXTRACT(YEAR FROM snapshot_date) = 2023
), _s1 AS (
  SELECT
    COUNT(tx_limit_daily) AS expr_1,
    COUNT(tx_limit_monthly) AS expr_3,
    SUM(tx_limit_daily) AS expr_0,
    SUM(tx_limit_monthly) AS expr_2,
    snapshot_date
  FROM main.user_setting_snapshot
  WHERE
    EXTRACT(YEAR FROM snapshot_date) = 2023
  GROUP BY
    snapshot_date
)
SELECT
  _s1.expr_0 / COALESCE(_s1.expr_1, 0) AS avg_daily_limit,
  _s1.expr_2 / COALESCE(_s1.expr_3, 0) AS avg_monthly_limit
FROM _s0 AS _s0
JOIN _s1 AS _s1
  ON _s0.min_date = _s1.snapshot_date
