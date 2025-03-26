SELECT
  SUM(table.a) AS a,
  table.b AS b
FROM table AS table
GROUP BY
  table.b
