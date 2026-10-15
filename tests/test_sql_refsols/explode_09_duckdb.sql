SELECT
  region.r_name AS name,
  COUNT(*) AS n_words,
  ARRAY_AGG(_s0.val) AS words_list
FROM tpch.region AS region
CROSS JOIN LATERAL (
  SELECT
    UNNEST(STR_SPLIT(region.r_comment, ' ')) AS _col_0,
    GENERATE_SUBSCRIPTS(STR_SPLIT(region.r_comment, ' '), 1) - 1 AS _col_1
) AS _s0(val, idx)
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
