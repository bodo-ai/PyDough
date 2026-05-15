SELECT
  infinty.py_float,
  infinty.np_float64,
  infinty.np_float32
FROM (VALUES
  (1.5, -2.25, 0.0),
  (NULL, NULL, NULL),
  (CAST('inf' AS REAL), CAST('inf' AS REAL), CAST('inf' AS REAL)),
  (CAST('-inf' AS REAL), CAST('-inf' AS REAL), CAST('-inf' AS REAL))) AS infinty(py_float, np_float64, np_float32)
