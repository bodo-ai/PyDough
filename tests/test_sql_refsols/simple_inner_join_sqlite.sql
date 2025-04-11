SELECT
  table.a,
  table_2.b
FROM table AS table
JOIN table AS table_2
  ON table.a = table_2.a
