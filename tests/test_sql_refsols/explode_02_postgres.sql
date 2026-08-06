SELECT
  tbl.key,
  tbl.arr,
  _l.idx - 1 AS arr_idx,
  _l.val AS arr_val
FROM (VALUES
  ('A', ARRAY[1]),
  ('B', (
      ARRAY[0]
  )[1 : 0]),
  ('C', ARRAY[2, 3, NULL, 4]),
  ('D', ARRAY[5, 6])) AS tbl(key, arr)
CROSS JOIN LATERAL UNNEST(tbl.arr) WITH ORDINALITY AS _l(val, idx)
ORDER BY
  1 NULLS FIRST,
  3 NULLS FIRST
