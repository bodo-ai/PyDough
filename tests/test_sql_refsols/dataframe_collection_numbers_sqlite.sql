SELECT
  column1 AS py_float,
  column2 AS np_float64,
  column3 AS np_float32,
  column4 AS null_vs_nan,
  column5 AS decimal_val
FROM (VALUES
  (1.5, 1.5, 1.5, NULL, 1.50),
  (0.0, 0.0, 3.33333, NULL, 0.00),
  (10.0001, 4.4444444, 0.0, NULL, -2.25),
  (-2.25, -2.25, -2.25, 1.0, NULL),
  (NULL, NULL, NULL, 0.0, NULL)) AS numbers
