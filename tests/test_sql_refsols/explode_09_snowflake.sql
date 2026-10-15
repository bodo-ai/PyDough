SELECT
  region.r_name AS name,
  COUNT(*) AS n_words,
  ARRAY_AGG(_s0.value) AS words_list
FROM tpch.region AS region
CROSS JOIN LATERAL SPLIT_TO_TABLE(region.r_comment, ' ') AS _s0
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
