SELECT
  rainbow.color,
  rainbow.idx
FROM (VALUES
  ('red', 0),
  ('orange', 1),
  ('yellow', 2),
  ('green', 3),
  ('blue', 4),
  ('indigo', 5),
  ('violet', 6),
  (NULL, 7)) AS rainbow(color, idx)
