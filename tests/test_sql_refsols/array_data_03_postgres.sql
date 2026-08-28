SELECT
  tbl.idx,
  tbl.arr_f
FROM (VALUES
  (1, ARRAY[1.1]),
  (2, (
      ARRAY[0]
  )[1 : 0]),
  (3, ARRAY[-2.3, 0.0]),
  (4, ARRAY[3.14, NULL, NULL, CAST('inf' AS REAL), CAST('-inf' AS REAL)])) AS tbl(idx, arr_f)
