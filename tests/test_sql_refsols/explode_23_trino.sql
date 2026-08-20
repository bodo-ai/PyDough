SELECT
  _s0.idx - 1 AS idx,
  _s0.val AS letter
FROM (VALUES
  (NULL)) AS _q_0(_col_0)
CROSS JOIN UNNEST(REGEXP_EXTRACT_ALL('ALPHABET', '.')) WITH ORDINALITY AS _s0(val, idx)
