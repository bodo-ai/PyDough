SELECT
  tbl.idx,
  tbl.arr_f
FROM (VALUES
  (1, [1.1]),
  (2, []),
  (3, [-2.3, 0.0]),
  (4, [3.14, NULL, NULL, CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE)])) AS tbl(idx, arr_f)
