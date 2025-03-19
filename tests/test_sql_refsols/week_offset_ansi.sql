SELECT
  date_time,
  DATE_ADD(DATETIME(date_time), 1, 'WEEK') AS week_adj1,
  DATE_ADD(DATETIME(date_time), -1, 'WEEK') AS week_adj2,
  DATE_ADD(DATE_ADD(DATETIME(date_time), 1, 'HOUR'), 2, 'WEEK') AS week_adj3,
  DATE_ADD(DATE_ADD(DATETIME(date_time), -1, 'SECOND'), 2, 'WEEK') AS week_adj4,
  DATE_ADD(DATE_ADD(DATETIME(date_time), 1, 'DAY'), 2, 'WEEK') AS week_adj5,
  DATE_ADD(DATE_ADD(DATETIME(date_time), -1, 'MINUTE'), 2, 'WEEK') AS week_adj6,
  DATE_ADD(DATE_ADD(DATETIME(date_time), 1, 'MONTH'), 2, 'WEEK') AS week_adj7,
  DATE_ADD(DATE_ADD(DATETIME(date_time), 1, 'YEAR'), 2, 'WEEK') AS week_adj8
FROM (
  SELECT
    date_time
  FROM (
    SELECT
      sbTxDateTime AS date_time
    FROM main.sbTransaction
  )
  WHERE
    (
      YEAR(date_time) < 2025
    ) AND (
      DAY(date_time) > 1
    )
)
