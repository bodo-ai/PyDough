SELECT
  column1 AS py_float,
  column2 AS np_float64,
  column3 AS np_float32
FROM (VALUES
  (1.5, -2.25, 0.0),
  (NULL, NULL, NULL),
  ('Infinity', 'Infinity', 'Infinity'),
  ('-Infinity', '-Infinity', '-Infinity')) AS infinty(py_float, np_float64, np_float32)
