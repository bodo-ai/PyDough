SELECT DISTINCT
  tbl.key,
  _s0.val AS arr_val
FROM (VALUES
  ('A', ARRAY[1]),
  ('B', (
      ARRAY[0]
  )[1 : 0]),
  ('C', ARRAY[2, 3, NULL, 4, NULL]),
  ('D', ARRAY[5, 6, 5])) AS tbl(key, arr)
CROSS JOIN LATERAL UNNEST(tbl.arr) WITH ORDINALITY AS _s0(val, idx)
