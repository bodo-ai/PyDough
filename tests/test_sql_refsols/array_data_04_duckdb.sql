SELECT
  tbl.idx,
  tbl.arr_d,
  tbl.arr_t
FROM (VALUES
  (1, [CAST('2020-01-01' AS TIMESTAMP)], [CAST('2020-01-01 12:00:00' AS TIMESTAMP)]),
  (2, [], []),
  (
    3,
    [CAST('2021-06-15' AS TIMESTAMP), CAST('2022-12-31' AS TIMESTAMP)],
    [
      CAST('2021-06-15 00:00:00' AS TIMESTAMP),
      CAST('2022-12-31 00:00:00' AS TIMESTAMP)
    ]
  ),
  (
    4,
    [CAST('1999-07-04' AS TIMESTAMP), NULL, CAST('2000-01-01' AS TIMESTAMP)],
    [
      CAST('1999-07-04 23:15:00' AS TIMESTAMP),
      NULL,
      CAST('2000-01-01 00:00:00' AS TIMESTAMP)
    ]
  )) AS tbl(idx, arr_d, arr_t)
