SELECT
  infinty.py_float,
  infinty.np_float64,
  infinty.np_float32
FROM (VALUES
  (1.5, -2.25, 0.0),
  (NULL, NULL, NULL),
  (TO_DOUBLE('INF'), TO_DOUBLE('INF'), TO_DOUBLE('INF')),
  (TO_DOUBLE('-INF'), TO_DOUBLE('-INF'), TO_DOUBLE('-INF'))) AS infinty(py_float, np_float64, np_float32)
