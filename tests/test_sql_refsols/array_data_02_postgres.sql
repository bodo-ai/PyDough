SELECT
  tbl.idx,
  tbl.arr_s,
  tbl.arr_i
FROM (VALUES
  (1, ARRAY['A'], ARRAY[10]),
  (2, (
      ARRAY['']
    )[1 : 0], (
      ARRAY[0]
  )[1 : 0]),
  (3, ARRAY['B', 'C'], ARRAY[20, 30]),
  (4, ARRAY['D', 'E', NULL, 'F'], ARRAY[40, 50, NULL, 60])) AS tbl(idx, arr_s, arr_i)
