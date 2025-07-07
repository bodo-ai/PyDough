SELECT
  FLOOR(5.6) AS floor_frac,
  CEIL(5.4) AS ceil_frac,
  FLOOR(-5.4) AS floor_frac_neg,
  CEIL(-5.6) AS ceil_frac_neg,
  FLOOR(6) AS floor_int,
  CEIL(6) AS ceil_int,
  FLOOR(-6) AS floor_int_neg,
  CEIL(-6) AS ceil_int_neg
FROM (VALUES
  (NULL)) AS _Q_0(_COL_0)
