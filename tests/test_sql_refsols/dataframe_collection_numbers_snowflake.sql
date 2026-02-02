SELECT
  numbers.pyd_numbers,
  numbers.py_float,
  numbers.np_float64,
  numbers.np_float32,
  numbers.null_vs_nan,
  numbers.decimal_val
FROM (VALUES
  (10.0, 1.5, 1.5, 1.5, NULL, 1.50),
  (-3.0, 0.0, 0.0, 3.33333, NULL, 0.00),
  (3.56, 10.0001, 4.4444444, 0.0, NULL, -2.25),
  (NULL, -2.25, -2.25, -2.25, 1.0, NULL),
  (NULL, NULL, NULL, NULL, 0.0, NULL)) AS numbers(pyd_numbers, py_float, np_float64, np_float32, null_vs_nan, decimal_val)
