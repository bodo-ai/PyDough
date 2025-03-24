SELECT
  date_time,
  DATE_ADD(CAST(date_time AS TIMESTAMP), 1, 'WEEK') AS week_adj1,
  DATE_ADD(CAST(date_time AS TIMESTAMP), -1, 'WEEK') AS week_adj2,
  DATE_ADD(DATE_ADD(CAST(date_time AS TIMESTAMP), 1, 'HOUR'), 2, 'WEEK') AS week_adj3,
  DATE_ADD(DATE_ADD(CAST(date_time AS TIMESTAMP), -1, 'SECOND'), 2, 'WEEK') AS week_adj4,
  DATE_ADD(DATE_ADD(CAST(date_time AS TIMESTAMP), 1, 'DAY'), 2, 'WEEK') AS week_adj5,
  DATE_ADD(DATE_ADD(CAST(date_time AS TIMESTAMP), -1, 'MINUTE'), 2, 'WEEK') AS week_adj6,
  DATE_ADD(DATE_ADD(CAST(date_time AS TIMESTAMP), 1, 'MONTH'), 2, 'WEEK') AS week_adj7,
  DATE_ADD(DATE_ADD(CAST(date_time AS TIMESTAMP), 1, 'YEAR'), 2, 'WEEK') AS week_adj8
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
      EXTRACT(YEAR FROM date_time) < 2025
    )
    AND (
      EXTRACT(DAY FROM date_time) > 1
    )
)
