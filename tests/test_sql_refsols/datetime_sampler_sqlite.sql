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
  DATETIME(order_date) AS _expr19,
  STRFTIME('%Y-%m-%d %H:%M:%S', DATETIME('now')) AS _expr20,
  DATETIME(DATE('now', 'start of year'), '8 minute', '-141 month') AS _expr21,
  STRFTIME('%Y-%m-%d %H:00:00', STRFTIME('%Y-%m-%d %H:%M:%S', DATE('now', 'start of month'))) AS _expr22,
  STRFTIME(
    '%Y-%m-%d %H:00:00',
    STRFTIME('%Y-%m-%d %H:%M:%S', STRFTIME('%Y-%m-%d %H:00:00', DATETIME('now')))
  ) AS _expr23,
  DATETIME('now', '-96 hour', '15 year') AS _expr24,
  DATETIME(STRFTIME('%Y-%m-%d %H:%M:00', DATE('now', 'start of year', '-3 year')), '65 month') AS _expr25,
  DATE(DATETIME(order_date, '-56 hour'), 'start of year') AS _expr26,
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
  DATE(DATETIME(order_date, '82 second', '415 second', '-160 second'), 'start of year') AS _expr41,
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
  DATETIME('now', '45 month', '-135 second') AS _expr58
FROM (
  SELECT
    o_orderdate AS order_date
  FROM tpch.ORDERS
)
