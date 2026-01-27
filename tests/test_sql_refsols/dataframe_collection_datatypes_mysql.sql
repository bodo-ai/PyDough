SELECT
  alldatatypes.string_col,
  alldatatypes.int_col,
  alldatatypes.float_col,
  alldatatypes.nullable_int_col,
  alldatatypes.bool_col,
  alldatatypes.null_col,
  alldatatypes.datetime_col
FROM (VALUES
  ROW('red', 0, 1.5, 1.0, 1, NULL, CAST('2024-01-01 00:00:00' AS DATETIME)),
  ROW('orange', 1, 2.0, NULL, 0, NULL, CAST('2024-01-02 00:00:00' AS DATETIME)),
  ROW(NULL, 2, NULL, 7.0, 0, NULL, NULL)) AS alldatatypes(string_col, int_col, float_col, nullable_int_col, bool_col, null_col, datetime_col)
