WITH _s1 AS (
  SELECT
    MIN(DATETIME(t_ts, '+54321 seconds')) AS min_time_stamp,
    t_sourceaccount
  FROM crbnk.transactions
  GROUP BY
    2
)
SELECT
  ROUND(
    AVG(
      (
        (
          CAST((
            JULIANDAY(DATE(_s1.min_time_stamp, 'start of day')) - JULIANDAY(DATE(DATETIME(accounts.a_open_ts, '+123456789 seconds'), 'start of day'))
          ) AS INTEGER) * 24 + CAST(STRFTIME('%H', _s1.min_time_stamp) AS INTEGER) - CAST(STRFTIME('%H', DATETIME(accounts.a_open_ts, '+123456789 seconds')) AS INTEGER)
        ) * 60 + CAST(STRFTIME('%M', _s1.min_time_stamp) AS INTEGER) - CAST(STRFTIME('%M', DATETIME(accounts.a_open_ts, '+123456789 seconds')) AS INTEGER)
      ) * 60 + CAST(STRFTIME('%S', _s1.min_time_stamp) AS INTEGER) - CAST(STRFTIME('%S', DATETIME(accounts.a_open_ts, '+123456789 seconds')) AS INTEGER)
    ),
    2
  ) AS avg_secs
FROM crbnk.accounts AS accounts
LEFT JOIN _s1 AS _s1
  ON _s1.t_sourceaccount = CASE
    WHEN accounts.a_key = 0
    THEN 0
    ELSE CASE WHEN accounts.a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(
      accounts.a_key,
      1 + INSTR(accounts.a_key, '-'),
      CAST(LENGTH(accounts.a_key) AS REAL) / 2
    ) AS INTEGER)
  END
