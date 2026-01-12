SELECT
  rainbow.color,
  rainbow.idx
FROM (VALUES
  ROW('red', 0),
  ROW('orange', 1),
  ROW('yellow', 2),
  ROW('green', 3),
  ROW('blue', 4),
  ROW('indigo', 5),
  ROW('violet', 6),
  ROW(NULL, 7)) AS rainbow(color, idx)
