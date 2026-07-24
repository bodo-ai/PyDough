SELECT
  rainbow.idx,
  rainbow.color
FROM (VALUES
  (0, 'red'),
  (1, 'orange'),
  (2, 'yellow'),
  (3, 'green'),
  (4, 'blue'),
  (5, 'indigo'),
  (6, 'violet'),
  (7, NULL)) AS rainbow(idx, color)
