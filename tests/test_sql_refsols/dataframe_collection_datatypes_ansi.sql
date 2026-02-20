SELECT
  column1 AS string_col,
  column2 AS int_col,
  column3 AS float_col,
  column4 AS nullable_int_col,
  column5 AS bool_col,
  column6 AS null_col,
  column7 AS datetime_col
FROM (VALUES
  ('red', 0, 1.5, 1.0, 1, NULL, CAST('2024-01-01 00:00:00' AS TIMESTAMP)),
  ('orange', 1, 2.0, NULL, 0, NULL, CAST('2024-01-02 00:00:00' AS TIMESTAMP)),
  (NULL, 2, NULL, 7.0, 0, NULL, NULL)) AS alldatatypes(string_col, int_col, float_col, nullable_int_col, bool_col, null_col, datetime_col)
