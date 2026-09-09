SELECT
  _s0.index AS idx,
  _s0.value AS letter
FROM (VALUES
  (NULL)) AS _q_0(_col_0)
CROSS JOIN LATERAL FLATTEN(REGEXP_EXTRACT_ALL('ALPHABET', '.{1}')) AS _s0(seq, key, path, index, value, this)
