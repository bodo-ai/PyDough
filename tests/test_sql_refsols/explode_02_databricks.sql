SELECT
  tbl.key,
  tbl.arr,
  _s0.idx AS arr_idx,
  _s0.val AS arr_val
FROM VALUES
  ('A', ARRAY(1)),
  ('B', ARRAY()),
  ('C', ARRAY(2, 3, NULL, 4)),
  ('D', ARRAY(5, 6)) AS tbl(key, arr)
CROSS JOIN LATERAL POSEXPLODE(tbl.arr) AS _s0(idx, val)
ORDER BY
  1,
  3
