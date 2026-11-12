SELECT
  region.r_name AS name,
  COUNT(*) AS n_words,
  COLLECT_LIST(_s0.val) AS words_list
FROM tpch_s3_pq.tpch_sf1.region AS region
CROSS JOIN LATERAL POSEXPLODE(SPLIT(region.r_comment, '\\Q \\E')) AS _s0(idx, val)
GROUP BY
  1
ORDER BY
  1
