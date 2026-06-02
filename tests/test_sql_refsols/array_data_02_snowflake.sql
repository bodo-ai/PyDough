SELECT
  tbl.idx,
  tbl.arr_f
FROM (VALUES
  (1, [1.1]),
  (2, []),
  (3, [-2.3, 0.0]),
  (4, [3.14, NULL, NULL, TO_DOUBLE('INF'), TO_DOUBLE('-INF')])) AS tbl(idx, arr_f)
