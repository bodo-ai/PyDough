SELECT
  COLUMN1 AS py_float,
  COLUMN2 AS np_float64,
  COLUMN3 AS np_float32
FROM (VALUES
  (1.5, -2.25, 0.0),
  (NULL, NULL, NULL),
  ('Infinity', 'Infinity', 'Infinity'),
  ('-Infinity', '-Infinity', '-Infinity')) AS INFINTY(PY_FLOAT, NP_FLOAT64, NP_FLOAT32)
