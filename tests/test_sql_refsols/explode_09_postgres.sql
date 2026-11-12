SELECT
  region.r_name AS name,
  COUNT(*) AS n_words,
  ARRAY_AGG(_s0.val) AS words_list
FROM tpch.region AS region
CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(region.r_comment, ' ')) WITH ORDINALITY AS _s0(val, idx)
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
