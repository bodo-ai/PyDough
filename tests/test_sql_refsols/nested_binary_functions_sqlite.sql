SELECT
  table.a * (
    table.b + 1
  ) AS a,
  table.a * (
    table.b + 1
  ) + table.b * 1 AS b
FROM table AS table
