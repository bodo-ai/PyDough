SELECT
  tbl.key,
  tbl.arr,
  _s0.idx AS arr_idx,
  _s0.val AS arr_val
FROM (VALUES
  ('A', [1]),
  ('B', []),
  ('C', [2, 3, NULL, 4]),
  ('D', [5, 6])) AS tbl(key, arr)
CROSS JOIN LATERAL (
  SELECT
    UNNEST(tbl.arr) AS _col_0,
    GENERATE_SUBSCRIPTS(tbl.arr, 1) - 1 AS _col_1
) AS _s0(val, idx)
ORDER BY
  1 NULLS FIRST,
  3 NULLS FIRST
