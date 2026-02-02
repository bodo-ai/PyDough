SELECT
  alldatatypes.column1 AS string_col,
  alldatatypes.column2 AS int_col,
  alldatatypes.column3 AS float_col,
  alldatatypes.column4 AS nullable_int_col,
  alldatatypes.column5 AS bool_col,
  alldatatypes.column6 AS null_col,
  alldatatypes.column7 AS datetime_col
FROM (VALUES
  ('red', 0, 1.5, 1.0, 1, NULL, '2024-01-01 00:00:00'),
  ('orange', 1, 2.0, NULL, 0, NULL, '2024-01-02 00:00:00'),
  (NULL, 2, NULL, 7.0, 0, NULL, NULL)) AS alldatatypes
