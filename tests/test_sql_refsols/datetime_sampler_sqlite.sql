SELECT
  DATETIME('2025-07-04 12:58:45') AS _expr0,
  DATETIME('2024-12-31 11:59:00') AS _expr1,
  DATETIME('2025-01-01') AS _expr2,
  DATETIME('1999-03-14') AS _expr3,
  DATETIME('now') AS _expr4,
  DATETIME('now') AS _expr5,
  DATETIME('now') AS _expr6,
  DATETIME('now') AS _expr7,
  DATETIME('now') AS _expr8,
  DATETIME('now') AS _expr9,
  DATETIME('now') AS _expr10,
  DATETIME('now') AS _expr11,
  DATETIME('now') AS _expr12,
  DATETIME('now') AS _expr13,
  DATETIME('now') AS _expr14,
  DATETIME('now') AS _expr15,
  DATETIME('now') AS _expr16,
  DATETIME('now') AS _expr17,
  DATETIME('now') AS _expr18,
  DATETIME(o_orderdate) AS _expr19,
  STRFTIME('%Y-%m-%d %H:%M:%S', DATETIME('now')) AS _expr20,
  DATETIME(DATE('now', 'start of year'), '8 minute', '-141 month') AS _expr21,
  STRFTIME('%Y-%m-%d %H:00:00', STRFTIME('%Y-%m-%d %H:%M:%S', DATE('now', 'start of month'))) AS _expr22,
  STRFTIME(
    '%Y-%m-%d %H:00:00',
    STRFTIME('%Y-%m-%d %H:%M:%S', STRFTIME('%Y-%m-%d %H:00:00', DATETIME('now')))
  ) AS _expr23,
  DATETIME('now', '-96 hour', '15 year') AS _expr24,
  DATETIME(STRFTIME('%Y-%m-%d %H:%M:00', DATE('now', 'start of year', '-3 year')), '65 month') AS _expr25,
  DATE(DATETIME(o_orderdate, '-56 hour'), 'start of year') AS _expr26,
  STRFTIME('%Y-%m-%d %H:%M:%S', STRFTIME('%Y-%m-%d %H:%M:00', DATETIME('now', '-63 day'))) AS _expr27,
  DATE('now', 'start of month') AS _expr28,
  DATETIME(STRFTIME('%Y-%m-%d %H:%M:%S', DATETIME('now', '-312 hour')), '48 year') AS _expr29,
  DATETIME(DATE(DATETIME('now', '75 day'), 'start of day'), '600 minute', '-294 day') AS _expr30,
  DATE('now', 'start of month', '480 month', '-45 year') AS _expr31,
  STRFTIME(
    '%Y-%m-%d %H:%M:%S',
    DATE(DATETIME('now', '-270 minute', '-34 second'), 'start of day')
  ) AS _expr32,
  DATETIME(DATE('now', 'start of month'), '213 second') AS _expr33,
  DATETIME(DATE('now', 'start of month'), '13 minute', '28 year', '344 second') AS _expr34,
  DATE('now', 'start of day') AS _expr35,
  DATETIME(STRFTIME('%Y-%m-%d %H:00:00', DATETIME('2025-01-01')), '49 minute', '91 year') AS _expr36,
  DATE('now', 'start of year', 'start of day') AS _expr37,
  DATE('now', 'start of day', 'start of year') AS _expr38,
  DATETIME(DATE('2025-07-04 12:58:45', 'start of month'), '22 minute') AS _expr39,
  DATE('now', 'start of year') AS _expr40,
  DATE(DATETIME(o_orderdate, '82 second', '415 second', '-160 second'), 'start of year') AS _expr41,
  DATETIME('now', '192 month') AS _expr42,
  DATETIME(
    STRFTIME(
      '%Y-%m-%d %H:00:00',
      STRFTIME('%Y-%m-%d %H:%M:00', STRFTIME('%Y-%m-%d %H:00:00', DATETIME('now')))
    ),
    '486 minute'
  ) AS _expr43,
  DATETIME(STRFTIME('%Y-%m-%d %H:%M:%S', DATETIME('now')), '-50 hour') AS _expr44,
  STRFTIME('%Y-%m-%d %H:00:00', DATETIME('now', '297 day', '72 month', '-92 month')) AS _expr45,
  DATE(DATETIME('now', '285 second'), 'start of day') AS _expr46,
  DATETIME('1999-03-14', '62 day') AS _expr47,
  DATE(DATETIME(DATE('now', 'start of month'), '1 hour'), 'start of month', '-21 day') AS _expr48,
  DATETIME('now', '212 minute', '368 year') AS _expr49,
  STRFTIME(
    '%Y-%m-%d %H:%M:00',
    STRFTIME(
      '%Y-%m-%d %H:%M:00',
      DATE('2024-12-31 11:59:00', 'start of month', 'start of year')
    )
  ) AS _expr50,
  DATE(STRFTIME('%Y-%m-%d %H:00:00', DATETIME('1999-03-14')), 'start of day') AS _expr51,
  DATETIME(
    STRFTIME('%Y-%m-%d %H:%M:00', DATE(DATETIME('now', '-60 hour'), 'start of day')),
    '196 year'
  ) AS _expr52,
  DATETIME(STRFTIME('%Y-%m-%d %H:%M:00', DATETIME('now', '-40 hour', '-385 day')), '29 hour') AS _expr53,
  STRFTIME('%Y-%m-%d %H:%M:00', STRFTIME('%Y-%m-%d %H:00:00', DATETIME('now', '405 day'))) AS _expr54,
  DATETIME(STRFTIME('%Y-%m-%d %H:%M:%S', DATE('now', 'start of year')), '98 year', '96 month') AS _expr55,
  DATETIME(
    DATE(
      STRFTIME('%Y-%m-%d %H:%M:%S', STRFTIME('%Y-%m-%d %H:%M:00', DATETIME('now'))),
      'start of day'
    ),
    '78 second'
  ) AS _expr56,
  DATETIME('now', '136 hour', '104 minute', '-104 month', '312 day') AS _expr57,
  DATETIME('now', '45 month', '-135 second') AS _expr58,
  CAST(STRFTIME('%Y', DATETIME('now')) AS INTEGER) AS _expr59,
  2025 AS _expr60,
  1999 AS _expr61,
  CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) AS _expr62,
  6 AS _expr63,
  3 AS _expr64,
  CAST(STRFTIME('%d', DATETIME('now')) AS INTEGER) AS _expr65,
  4 AS _expr66,
  4 AS _expr67,
  CAST(STRFTIME('%H', DATETIME('now')) AS INTEGER) AS _expr68,
  0 AS _expr69,
  0 AS _expr70,
  CAST(STRFTIME('%M', DATETIME('now')) AS INTEGER) AS _expr71,
  30 AS _expr72,
  0 AS _expr73,
  CAST(STRFTIME('%S', DATETIME('now')) AS INTEGER) AS _expr74,
  45 AS _expr75,
  0 AS _expr76,
  CAST(STRFTIME('%Y', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%Y', DATETIME('2018-02-14 12:41:06')) AS INTEGER) AS _expr77,
  CAST(STRFTIME('%Y', '2022-11-24') AS INTEGER) - CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS _expr78,
  (
    CAST(STRFTIME('%Y', DATETIME('1999-03-14')) AS INTEGER) - CAST(STRFTIME('%Y', '2005-06-30') AS INTEGER)
  ) * 12 + CAST(STRFTIME('%m', DATETIME('1999-03-14')) AS INTEGER) - CAST(STRFTIME('%m', '2005-06-30') AS INTEGER) AS _expr79,
  (
    CAST(STRFTIME('%Y', '2022-11-24') AS INTEGER) - CAST(STRFTIME('%Y', '2006-05-01 12:00:00') AS INTEGER)
  ) * 12 + CAST(STRFTIME('%m', '2022-11-24') AS INTEGER) - CAST(STRFTIME('%m', '2006-05-01 12:00:00') AS INTEGER) AS _expr80,
  CAST((
    JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(DATETIME('now'), 'start of day'))
  ) AS INTEGER) AS _expr81,
  CAST((
    JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(DATETIME('1999-03-14'), 'start of day'))
  ) AS INTEGER) AS _expr82,
  CAST((
    JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(DATETIME('now'), 'start of day'))
  ) AS INTEGER) * 24 + CAST(STRFTIME('%H', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%H', DATETIME('now')) AS INTEGER) AS _expr83,
  CAST((
    JULIANDAY(DATE(o_orderdate, 'start of day')) - JULIANDAY(DATE('2005-06-30', 'start of day'))
  ) AS INTEGER) * 24 + CAST(STRFTIME('%H', o_orderdate) AS INTEGER) - CAST(STRFTIME('%H', '2005-06-30') AS INTEGER) AS _expr84,
  (
    CAST((
      JULIANDAY(DATE('2006-05-01 12:00:00', 'start of day')) - JULIANDAY(DATE(DATETIME('now'), 'start of day'))
    ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2006-05-01 12:00:00') AS INTEGER) - CAST(STRFTIME('%H', DATETIME('now')) AS INTEGER)
  ) * 60 + CAST(STRFTIME('%M', '2006-05-01 12:00:00') AS INTEGER) - CAST(STRFTIME('%M', DATETIME('now')) AS INTEGER) AS _expr85,
  (
    CAST((
      JULIANDAY(DATE('2021-01-01 07:35:00', 'start of day')) - JULIANDAY(DATE(o_orderdate, 'start of day'))
    ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2021-01-01 07:35:00') AS INTEGER) - CAST(STRFTIME('%H', o_orderdate) AS INTEGER)
  ) * 60 + CAST(STRFTIME('%M', '2021-01-01 07:35:00') AS INTEGER) - CAST(STRFTIME('%M', o_orderdate) AS INTEGER) AS _expr86,
  (
    (
      CAST((
        JULIANDAY(DATE('2021-01-01 07:35:00', 'start of day')) - JULIANDAY(DATE('2022-11-24', 'start of day'))
      ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2021-01-01 07:35:00') AS INTEGER) - CAST(STRFTIME('%H', '2022-11-24') AS INTEGER)
    ) * 60 + CAST(STRFTIME('%M', '2021-01-01 07:35:00') AS INTEGER) - CAST(STRFTIME('%M', '2022-11-24') AS INTEGER)
  ) * 60 + CAST(STRFTIME('%S', '2021-01-01 07:35:00') AS INTEGER) - CAST(STRFTIME('%S', '2022-11-24') AS INTEGER) AS _expr87,
  (
    (
      CAST((
        JULIANDAY(DATE(DATETIME('2018-02-14 12:41:06'), 'start of day')) - JULIANDAY(DATE('2005-06-30', 'start of day'))
      ) AS INTEGER) * 24 + CAST(STRFTIME('%H', DATETIME('2018-02-14 12:41:06')) AS INTEGER) - CAST(STRFTIME('%H', '2005-06-30') AS INTEGER)
    ) * 60 + CAST(STRFTIME('%M', DATETIME('2018-02-14 12:41:06')) AS INTEGER) - CAST(STRFTIME('%M', '2005-06-30') AS INTEGER)
  ) * 60 + CAST(STRFTIME('%S', DATETIME('2018-02-14 12:41:06')) AS INTEGER) - CAST(STRFTIME('%S', '2005-06-30') AS INTEGER) AS _expr88,
  CAST(STRFTIME('%Y', '2006-05-01 12:00:00') AS INTEGER) - CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS _expr89,
  CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) - CAST(STRFTIME('%Y', DATETIME('2018-02-14 12:41:06')) AS INTEGER) AS _expr90,
  (
    CAST(STRFTIME('%Y', '2019-07-04 11:30:00') AS INTEGER) - CAST(STRFTIME('%Y', o_orderdate) AS INTEGER)
  ) * 12 + CAST(STRFTIME('%m', '2019-07-04 11:30:00') AS INTEGER) - CAST(STRFTIME('%m', o_orderdate) AS INTEGER) AS _expr91,
  (
    CAST(STRFTIME('%Y', DATETIME('2018-02-14 12:41:06')) AS INTEGER) - CAST(STRFTIME('%Y', '2019-07-04 11:30:00') AS INTEGER)
  ) * 12 + CAST(STRFTIME('%m', DATETIME('2018-02-14 12:41:06')) AS INTEGER) - CAST(STRFTIME('%m', '2019-07-04 11:30:00') AS INTEGER) AS _expr92,
  CAST((
    JULIANDAY(DATE(o_orderdate, 'start of day')) - JULIANDAY(DATE(DATETIME('now'), 'start of day'))
  ) AS INTEGER) AS _expr93,
  CAST((
    JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE('2019-07-04 11:30:00', 'start of day'))
  ) AS INTEGER) AS _expr94,
  CAST((
    JULIANDAY(DATE(DATETIME('1999-03-14'), 'start of day')) - JULIANDAY(DATE('2022-11-24', 'start of day'))
  ) AS INTEGER) * 24 + CAST(STRFTIME('%H', DATETIME('1999-03-14')) AS INTEGER) - CAST(STRFTIME('%H', '2022-11-24') AS INTEGER) AS _expr95,
  CAST((
    JULIANDAY(DATE('2020-12-31 00:31:06', 'start of day')) - JULIANDAY(DATE(DATETIME('2018-02-14 12:41:06'), 'start of day'))
  ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2020-12-31 00:31:06') AS INTEGER) - CAST(STRFTIME('%H', DATETIME('2018-02-14 12:41:06')) AS INTEGER) AS _expr96,
  (
    CAST((
      JULIANDAY(DATE('2020-12-31 00:31:06', 'start of day')) - JULIANDAY(DATE('2005-06-30', 'start of day'))
    ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2020-12-31 00:31:06') AS INTEGER) - CAST(STRFTIME('%H', '2005-06-30') AS INTEGER)
  ) * 60 + CAST(STRFTIME('%M', '2020-12-31 00:31:06') AS INTEGER) - CAST(STRFTIME('%M', '2005-06-30') AS INTEGER) AS _expr97,
  (
    CAST((
      JULIANDAY(DATE(DATETIME('2018-02-14 12:41:06'), 'start of day')) - JULIANDAY(DATE(DATETIME('now'), 'start of day'))
    ) AS INTEGER) * 24 + CAST(STRFTIME('%H', DATETIME('2018-02-14 12:41:06')) AS INTEGER) - CAST(STRFTIME('%H', DATETIME('now')) AS INTEGER)
  ) * 60 + CAST(STRFTIME('%M', DATETIME('2018-02-14 12:41:06')) AS INTEGER) - CAST(STRFTIME('%M', DATETIME('now')) AS INTEGER) AS _expr98,
  (
    (
      CAST((
        JULIANDAY(DATE(DATETIME('1999-03-14'), 'start of day')) - JULIANDAY(DATE(DATETIME('now'), 'start of day'))
      ) AS INTEGER) * 24 + CAST(STRFTIME('%H', DATETIME('1999-03-14')) AS INTEGER) - CAST(STRFTIME('%H', DATETIME('now')) AS INTEGER)
    ) * 60 + CAST(STRFTIME('%M', DATETIME('1999-03-14')) AS INTEGER) - CAST(STRFTIME('%M', DATETIME('now')) AS INTEGER)
  ) * 60 + CAST(STRFTIME('%S', DATETIME('1999-03-14')) AS INTEGER) - CAST(STRFTIME('%S', DATETIME('now')) AS INTEGER) AS _expr99,
  (
    (
      CAST((
        JULIANDAY(DATE('2019-07-04 11:30:00', 'start of day')) - JULIANDAY(DATE('2022-11-24', 'start of day'))
      ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2019-07-04 11:30:00') AS INTEGER) - CAST(STRFTIME('%H', '2022-11-24') AS INTEGER)
    ) * 60 + CAST(STRFTIME('%M', '2019-07-04 11:30:00') AS INTEGER) - CAST(STRFTIME('%M', '2022-11-24') AS INTEGER)
  ) * 60 + CAST(STRFTIME('%S', '2019-07-04 11:30:00') AS INTEGER) - CAST(STRFTIME('%S', '2022-11-24') AS INTEGER) AS _expr100
FROM tpch.orders
