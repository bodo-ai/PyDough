SELECT
  INFINTY.PY_FLOAT AS py_float,
  INFINTY.NP_FLOAT64 AS np_float64,
  INFINTY.NP_FLOAT32 AS np_float32
FROM (VALUES
  (1.5, -2.25, 0.0),
  (NULL, NULL, NULL),
  ('Infinity', 'Infinity', 'Infinity'),
  ('-Infinity', '-Infinity', '-Infinity')) AS INFINTY(PY_FLOAT, NP_FLOAT64, NP_FLOAT32)
