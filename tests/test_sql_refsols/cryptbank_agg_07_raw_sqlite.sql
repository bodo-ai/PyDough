SELECT
  COALESCE(
    SUM(DATE(DATETIME(t_ts, '+54321 seconds'), 'start of year') = DATE('2023-01-01')),
    0
  ) AS n_yr,
  COALESCE(
    SUM(
      DATE(
        DATETIME(t_ts, '+54321 seconds'),
        'start of month',
        '-' || CAST((
          (
            CAST(STRFTIME('%m', DATETIME(DATETIME(t_ts, '+54321 seconds'))) AS INTEGER) - 1
          ) % 3
        ) AS TEXT) || ' months'
      ) = DATE('2023-04-01')
    ),
    0
  ) AS n_qu,
  COALESCE(
    SUM(DATE(DATETIME(t_ts, '+54321 seconds'), 'start of month') = DATE('2023-06-01')),
    0
  ) AS n_mo,
  COALESCE(
    SUM(
      DATE(
        DATETIME(t_ts, '+54321 seconds'),
        '-' || CAST(CAST(STRFTIME('%w', DATETIME(DATETIME(t_ts, '+54321 seconds'))) AS INTEGER) AS TEXT) || ' days',
        'start of day'
      ) = DATE('2023-05-28')
    ),
    0
  ) AS n_we,
  COALESCE(
    SUM(DATE(DATETIME(t_ts, '+54321 seconds'), 'start of day') = DATE('2023-06-02')),
    0
  ) AS n_da,
  COALESCE(
    SUM(
      STRFTIME('%Y-%m-%d %H:00:00', DATETIME(DATETIME(t_ts, '+54321 seconds'))) = '2023-06-02 04:00:00'
    ),
    0
  ) AS n_ho,
  COALESCE(
    SUM(
      STRFTIME('%Y-%m-%d %H:%M:00', DATETIME(DATETIME(t_ts, '+54321 seconds'))) = '2023-06-02 04:55:00'
    ),
    0
  ) AS n_mi,
  COALESCE(
    SUM(
      STRFTIME('%Y-%m-%d %H:%M:%S', DATETIME(DATETIME(t_ts, '+54321 seconds'))) = '2023-06-02 04:55:31'
    ),
    0
  ) AS n_se,
  COALESCE(SUM(DATE('now', 'start of day') = DATETIME(t_ts, '+54321 seconds')), 0) AS n_cts,
  COALESCE(SUM(DATETIME('2025-12-31') = DATETIME(t_ts, '+54321 seconds')), 0) AS n_dts,
  COALESCE(
    SUM(
      DATE(
        DATETIME(t_ts, '+54321 seconds'),
        '-' || CAST(CAST(STRFTIME('%w', DATETIME(DATETIME(t_ts, '+54321 seconds'))) AS INTEGER) AS TEXT) || ' days',
        'start of day',
        '3 day'
      ) = DATE('2023-05-31')
    ),
    0
  ) AS n_nst,
  COALESCE(
    SUM(DATETIME(DATETIME(t_ts, '+54321 seconds'), '1 year') = '2020-11-11 18:00:52'),
    0
  ) AS n_ayr,
  COALESCE(
    SUM(DATETIME(DATETIME(t_ts, '+54321 seconds'), '6 month') = '2020-05-11 18:00:52'),
    0
  ) AS n_aqu,
  COALESCE(
    SUM(DATETIME(DATETIME(t_ts, '+54321 seconds'), '-5 month') = '2019-06-11 18:00:52'),
    0
  ) AS n_amo,
  COALESCE(
    SUM(
      DATE(DATETIME(t_ts, '+54321 seconds'), 'start of day', '7 day') = DATE('2023-06-09')
    ),
    0
  ) AS n_awe,
  COALESCE(
    SUM(DATETIME(DATETIME(t_ts, '+54321 seconds'), '10 day') = '2019-11-21 18:00:52'),
    0
  ) AS n_ada,
  COALESCE(
    SUM(DATETIME(DATETIME(t_ts, '+54321 seconds'), '1000 hour') = '2019-12-23 10:00:52'),
    0
  ) AS n_aho,
  COALESCE(
    SUM(
      DATETIME(DATETIME(t_ts, '+54321 seconds'), '10000 minute') = '2019-11-18 16:40:52'
    ),
    0
  ) AS n_ami,
  COALESCE(
    SUM(
      DATETIME(DATETIME(t_ts, '+54321 seconds'), '-1000000 second') = '2019-10-31 04:14:12'
    ),
    0
  ) AS n_ase,
  COALESCE(
    SUM(
      DATE(DATETIME(t_ts, '+54321 seconds'), 'start of month', '-1 day') = DATE('2019-10-31')
    ),
    0
  ) AS n_ldm
FROM crbnk.transactions
