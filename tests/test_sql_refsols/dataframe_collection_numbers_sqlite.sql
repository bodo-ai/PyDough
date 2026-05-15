SELECT
  numbers.column1 AS pyd_numbers,
  numbers.column2 AS py_float,
  numbers.column3 AS np_float64,
  numbers.column4 AS np_float32,
  numbers.column5 AS null_vs_nan,
  numbers.column6 AS decimal_val
FROM (VALUES
  (10.0, 1.5, 1.5, 1.5, NULL, 1.50),
  (-3.0, 0.0, 0.0, 3.33333, NULL, 0.00),
  (3.56, 10.0001, 4.4444444, 0.0, NULL, -2.25),
  (NULL, -2.25, -2.25, -2.25, 1.0, NULL),
  (NULL, NULL, NULL, NULL, 0.0, NULL)) AS numbers
