SELECT
  _s0.idx,
  _s0.val AS letter
FROM VALUES
  (NULL) AS _q_0(_col_0), LATERAL POSEXPLODE(SPLIT('ALPHABET', '\\Q\\E')) AS _s0(idx, val)
WHERE
  _s0.val <> ''
