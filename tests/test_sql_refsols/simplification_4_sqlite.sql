WITH _t AS (
  SELECT
    sbtxdatetime,
    ROW_NUMBER() OVER (ORDER BY sbtxdatetime) AS _w,
    ROW_NUMBER() OVER (ORDER BY sbtxdatetime DESC) AS _w_2
  FROM main.sbtransaction
  WHERE
    CAST(STRFTIME('%Y', sbtxdatetime) AS INTEGER) = 2023
)
SELECT
  sbtxdatetime AS date_time,
  DATE(
    sbtxdatetime,
    '-' || CAST((
      CAST(STRFTIME('%w', DATETIME(sbtxdatetime)) AS INTEGER) + 6
    ) % 7 AS TEXT) || ' days',
    'start of day',
    '-56 day'
  ) AS s00,
  CASE WHEN NOT sbtxdatetime IS NULL THEN FALSE ELSE NULL END AS s01,
  CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) IN (1, 2, 3) AS s02,
  CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) IN (4, 5, 6) AS s03,
  CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) IN (7, 8, 9) AS s04,
  CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) IN (10, 11, 12) AS s05,
  CASE WHEN NOT sbtxdatetime IS NULL THEN FALSE ELSE NULL END AS s06,
  CASE WHEN NOT sbtxdatetime IS NULL THEN FALSE ELSE NULL END AS s07,
  CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) < 4 AS s08,
  CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) < 7 AS s09,
  CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) < 10 AS s10,
  CASE WHEN NOT sbtxdatetime IS NULL THEN TRUE ELSE NULL END AS s11,
  CASE WHEN NOT sbtxdatetime IS NULL THEN FALSE ELSE NULL END AS s12,
  CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) <= 3 AS s13,
  CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) <= 6 AS s14,
  CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) <= 9 AS s15,
  CASE WHEN NOT sbtxdatetime IS NULL THEN TRUE ELSE NULL END AS s16,
  CASE WHEN NOT sbtxdatetime IS NULL THEN TRUE ELSE NULL END AS s17,
  CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) > 3 AS s18,
  CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) > 6 AS s19,
  CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) > 9 AS s20,
  CASE WHEN NOT sbtxdatetime IS NULL THEN FALSE ELSE NULL END AS s21,
  CASE WHEN NOT sbtxdatetime IS NULL THEN TRUE ELSE NULL END AS s22,
  CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) >= 4 AS s23,
  CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) >= 7 AS s24,
  CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) >= 10 AS s25,
  CASE WHEN NOT sbtxdatetime IS NULL THEN FALSE ELSE NULL END AS s26,
  CASE WHEN NOT sbtxdatetime IS NULL THEN TRUE ELSE NULL END AS s27,
  NOT CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) IN (1, 2, 3) AS s28,
  NOT CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) IN (4, 5, 6) AS s29,
  NOT CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) IN (7, 8, 9) AS s30,
  NOT CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) IN (10, 11, 12) AS s31,
  CASE WHEN NOT sbtxdatetime IS NULL THEN TRUE ELSE NULL END AS s32,
  2024 AS s33,
  3 AS s34,
  8 AS s35,
  13 AS s36,
  12 AS s37,
  45 AS s38,
  59 AS s39,
  2020 AS s40,
  1 AS s41,
  1 AS s42,
  31 AS s43,
  0 AS s44,
  0 AS s45,
  0 AS s46,
  2023 AS s47,
  3 AS s48,
  7 AS s49,
  4 AS s50,
  6 AS s51,
  55 AS s52,
  0 AS s53,
  1999 AS s54,
  4 AS s55,
  12 AS s56,
  31 AS s57,
  23 AS s58,
  59 AS s59,
  58 AS s60
FROM _t
WHERE
  _w = 1 OR _w_2 = 1
