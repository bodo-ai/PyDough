WITH _s1 AS (
  SELECT
    MIN(t_ts) AS min_t_ts,
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
            JULIANDAY(DATE(_s1.min_t_ts, 'start of day')) - JULIANDAY(DATE(accounts.a_open_ts, 'start of day'))
          ) AS INTEGER) * 24 + CAST(STRFTIME('%H', _s1.min_t_ts) AS INTEGER) - CAST(STRFTIME('%H', accounts.a_open_ts) AS INTEGER)
        ) * 60 + CAST(STRFTIME('%M', _s1.min_t_ts) AS INTEGER) - CAST(STRFTIME('%M', accounts.a_open_ts) AS INTEGER)
      ) * 60 + CAST(STRFTIME('%S', _s1.min_t_ts) AS INTEGER) - CAST(STRFTIME('%S', accounts.a_open_ts) AS INTEGER)
    ),
    2
  ) AS avg_secs
FROM crbnk.accounts AS accounts
LEFT JOIN _s1 AS _s1
  ON _s1.t_sourceaccount = accounts.a_key
