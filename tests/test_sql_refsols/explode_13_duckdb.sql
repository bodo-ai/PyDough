SELECT DISTINCT
  tbl.key,
  _s0.val AS arr_val
FROM (VALUES
  ('A', [1]),
  ('B', []),
  ('C', [2, 3, NULL, 4, NULL]),
  ('D', [5, 6, 5])) AS tbl(key, arr)
CROSS JOIN LATERAL (
  SELECT
    UNNEST(tbl.arr) AS _col_0,
    GENERATE_SUBSCRIPTS(tbl.arr, 1) - 1 AS _col_1
) AS _s0(val, idx)
