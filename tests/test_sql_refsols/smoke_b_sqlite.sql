SELECT
  o_orderkey AS key,
  CONCAT_WS(
    '_',
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER),
    CASE
      WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) <= 3
      AND CAST(STRFTIME('%m', o_orderdate) AS INTEGER) >= 1
      THEN 1
      WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) <= 6
      AND CAST(STRFTIME('%m', o_orderdate) AS INTEGER) >= 4
      THEN 2
      WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) <= 9
      AND CAST(STRFTIME('%m', o_orderdate) AS INTEGER) >= 7
      THEN 3
      WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) <= 12
      AND CAST(STRFTIME('%m', o_orderdate) AS INTEGER) >= 10
      THEN 4
    END,
    CAST(STRFTIME('%m', o_orderdate) AS INTEGER),
    CAST(STRFTIME('%d', o_orderdate) AS INTEGER)
  ) AS a,
  CONCAT_WS(
    ':',
    CASE
      WHEN CAST(STRFTIME('%w', o_orderdate) AS INTEGER) = 0
      THEN 'Sunday'
      WHEN CAST(STRFTIME('%w', o_orderdate) AS INTEGER) = 1
      THEN 'Monday'
      WHEN CAST(STRFTIME('%w', o_orderdate) AS INTEGER) = 2
      THEN 'Tuesday'
      WHEN CAST(STRFTIME('%w', o_orderdate) AS INTEGER) = 3
      THEN 'Wednesday'
      WHEN CAST(STRFTIME('%w', o_orderdate) AS INTEGER) = 4
      THEN 'Thursday'
      WHEN CAST(STRFTIME('%w', o_orderdate) AS INTEGER) = 5
      THEN 'Friday'
      WHEN CAST(STRFTIME('%w', o_orderdate) AS INTEGER) = 6
      THEN 'Saturday'
    END,
    CAST(STRFTIME('%w', o_orderdate) AS INTEGER)
  ) AS b,
  DATE(o_orderdate, 'start of year', '6 month', '-13 day') AS c,
  DATETIME(
    DATE(
      o_orderdate,
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(o_orderdate)) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months',
      '1 year'
    ),
    '25 hour'
  ) AS d,
  STRFTIME('%Y-%m-%d %H:%M:00', DATETIME('2025-01-01 12:35:13')) AS e,
  DATETIME(STRFTIME('%Y-%m-%d %H:00:00', DATETIME('2025-01-01 12:35:13')), '6 month', '21 day') AS f,
  DATE('2025-01-01 12:35:13', 'start of day') AS g,
  CONCAT_WS(
    ';',
    12,
    CAST(STRFTIME('%M', DATETIME('2025-01-01 12:35:13', '45 minute')) AS INTEGER),
    CAST(STRFTIME('%S', DATETIME('2025-01-01 12:35:13', '-7 second')) AS INTEGER)
  ) AS h,
  CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) - CAST(STRFTIME('%Y', DATETIME('1993-05-25 12:45:36')) AS INTEGER) AS i,
  (
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) - CAST(STRFTIME('%Y', DATETIME('1993-05-25 12:45:36')) AS INTEGER)
  ) * 4 + CASE
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) <= 3
    AND CAST(STRFTIME('%m', o_orderdate) AS INTEGER) >= 1
    THEN 1
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) <= 6
    AND CAST(STRFTIME('%m', o_orderdate) AS INTEGER) >= 4
    THEN 2
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) <= 9
    AND CAST(STRFTIME('%m', o_orderdate) AS INTEGER) >= 7
    THEN 3
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) <= 12
    AND CAST(STRFTIME('%m', o_orderdate) AS INTEGER) >= 10
    THEN 4
  END - CASE
    WHEN CAST(STRFTIME('%m', DATETIME('1993-05-25 12:45:36')) AS INTEGER) <= 3
    AND CAST(STRFTIME('%m', DATETIME('1993-05-25 12:45:36')) AS INTEGER) >= 1
    THEN 1
    WHEN CAST(STRFTIME('%m', DATETIME('1993-05-25 12:45:36')) AS INTEGER) <= 6
    AND CAST(STRFTIME('%m', DATETIME('1993-05-25 12:45:36')) AS INTEGER) >= 4
    THEN 2
    WHEN CAST(STRFTIME('%m', DATETIME('1993-05-25 12:45:36')) AS INTEGER) <= 9
    AND CAST(STRFTIME('%m', DATETIME('1993-05-25 12:45:36')) AS INTEGER) >= 7
    THEN 3
    WHEN CAST(STRFTIME('%m', DATETIME('1993-05-25 12:45:36')) AS INTEGER) <= 12
    AND CAST(STRFTIME('%m', DATETIME('1993-05-25 12:45:36')) AS INTEGER) >= 10
    THEN 4
  END AS j,
  (
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) - CAST(STRFTIME('%Y', DATETIME('1993-05-25 12:45:36')) AS INTEGER)
  ) * 12 + CAST(STRFTIME('%m', o_orderdate) AS INTEGER) - CAST(STRFTIME('%m', DATETIME('1993-05-25 12:45:36')) AS INTEGER) AS k,
  CAST(CAST(CAST((
    JULIANDAY(
      DATE(
        o_orderdate,
        '-' || CAST(CAST(STRFTIME('%w', DATETIME(o_orderdate)) AS INTEGER) AS TEXT) || ' days',
        'start of day'
      )
    ) - JULIANDAY(
      DATE(
        DATETIME('1993-05-25 12:45:36'),
        '-' || CAST(CAST(STRFTIME('%w', DATETIME(DATETIME('1993-05-25 12:45:36'))) AS INTEGER) AS TEXT) || ' days',
        'start of day'
      )
    )
  ) AS INTEGER) AS REAL) / 7 AS INTEGER) AS l,
  CAST((
    JULIANDAY(DATE(o_orderdate, 'start of day')) - JULIANDAY(DATE(DATETIME('1993-05-25 12:45:36'), 'start of day'))
  ) AS INTEGER) AS m,
  CAST((
    JULIANDAY(DATE(o_orderdate, 'start of day')) - JULIANDAY(DATE(DATETIME('1993-05-25 12:45:36'), 'start of day'))
  ) AS INTEGER) * 24 + CAST(STRFTIME('%H', o_orderdate) AS INTEGER) - CAST(STRFTIME('%H', DATETIME('1993-05-25 12:45:36')) AS INTEGER) AS n,
  (
    CAST((
      JULIANDAY(DATE(o_orderdate, 'start of day')) - JULIANDAY(DATE(DATETIME('1993-05-25 12:45:36'), 'start of day'))
    ) AS INTEGER) * 24 + CAST(STRFTIME('%H', o_orderdate) AS INTEGER) - CAST(STRFTIME('%H', DATETIME('1993-05-25 12:45:36')) AS INTEGER)
  ) * 60 + CAST(STRFTIME('%M', o_orderdate) AS INTEGER) - CAST(STRFTIME('%M', DATETIME('1993-05-25 12:45:36')) AS INTEGER) AS o,
  (
    (
      CAST((
        JULIANDAY(DATE(o_orderdate, 'start of day')) - JULIANDAY(DATE(DATETIME('1993-05-25 12:45:36'), 'start of day'))
      ) AS INTEGER) * 24 + CAST(STRFTIME('%H', o_orderdate) AS INTEGER) - CAST(STRFTIME('%H', DATETIME('1993-05-25 12:45:36')) AS INTEGER)
    ) * 60 + CAST(STRFTIME('%M', o_orderdate) AS INTEGER) - CAST(STRFTIME('%M', DATETIME('1993-05-25 12:45:36')) AS INTEGER)
  ) * 60 + CAST(STRFTIME('%S', o_orderdate) AS INTEGER) - CAST(STRFTIME('%S', DATETIME('1993-05-25 12:45:36')) AS INTEGER) AS p,
  DATE(
    o_orderdate,
    '-' || CAST(CAST(STRFTIME('%w', DATETIME(o_orderdate)) AS INTEGER) AS TEXT) || ' days',
    'start of day'
  ) AS q
FROM tpch.orders
WHERE
  o_clerk LIKE '%5' AND o_comment LIKE '%fo%' AND o_orderpriority LIKE '3%'
ORDER BY
  o_orderkey
LIMIT 5
