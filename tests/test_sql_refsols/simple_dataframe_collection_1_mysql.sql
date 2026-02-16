SELECT
  rainbow.idx,
  rainbow.color
FROM (VALUES
  ROW(0, 'red'),
  ROW(1, 'orange'),
  ROW(2, 'yellow'),
  ROW(3, 'green'),
  ROW(4, 'blue'),
  ROW(5, 'indigo'),
  ROW(6, 'violet'),
  ROW(7, NULL)) AS rainbow(idx, color)
