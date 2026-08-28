SELECT
  tbl.idx,
  tbl.arr_s,
  tbl.arr_i
FROM (VALUES
  (1, ['A'], [10]),
  (2, [], []),
  (3, ['B', 'C'], [20, 30]),
  (4, ['D', 'E', NULL, 'F'], [40, 50, NULL, 60])) AS tbl(idx, arr_s, arr_i)
