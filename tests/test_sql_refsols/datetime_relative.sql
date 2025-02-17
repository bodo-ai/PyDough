SELECT
  d1,
  d2,
  d3,
  d4,
  d5,
  d6
FROM (
  SELECT
    DATE_ADD(
      DATE_ADD(
        DATE_ADD(
          DATE_ADD(DATE_ADD(DATE_ADD(DATETIME(order_date), -11, 'YEAR'), 9, 'MONTH'), -7, 'DAY'),
          5,
          'HOUR'
        ),
        -3,
        'MINUTE'
      ),
      1,
      'SECOND'
    ) AS d3,
    DATE_ADD(DATETIME(TIME_STR_TO_TIME('2025-07-14 12:58:45')), 1000000, 'SECOND') AS d6,
    DATE_TRUNC('MONTH', DATETIME(order_date)) AS d2,
    DATE_TRUNC('YEAR', DATETIME(order_date)) AS d1,
    DATE_TRUNC('HOUR', DATETIME(TIME_STR_TO_TIME('2025-07-04 12:58:45'))) AS d4,
    DATE_TRUNC('MINUTE', DATETIME(TIME_STR_TO_TIME('2025-07-04 12:58:45'))) AS d5,
    ordering_2
  FROM (
    SELECT
      order_date AS ordering_2,
      order_date
    FROM (
      SELECT
        o_custkey AS ordering_0,
        o_orderdate AS order_date,
        o_orderdate AS ordering_1
      FROM tpch.ORDERS
    )
    ORDER BY
      ordering_0,
      ordering_1
    LIMIT 10
  )
)
ORDER BY
  ordering_2
