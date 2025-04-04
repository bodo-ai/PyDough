SELECT
  table.a AS a,
  table_2.b AS b
FROM table AS table
JOIN table AS table_2
  ON table.a = table_2.a
