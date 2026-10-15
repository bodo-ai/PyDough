SELECT
  tbl.idx,
  tbl.arr_s,
  tbl.arr_i
FROM (VALUES
  ROW(1, JSON_ARRAY('A'), JSON_ARRAY(10)),
  ROW(2, JSON_ARRAY(), JSON_ARRAY()),
  ROW(3, JSON_ARRAY('B', 'C'), JSON_ARRAY(20, 30)),
  ROW(4, JSON_ARRAY('D', 'E', NULL, 'F'), JSON_ARRAY(40, 50, NULL, 60))) AS tbl(idx, arr_s, arr_i)
