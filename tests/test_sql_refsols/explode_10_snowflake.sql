SELECT
  LENGTH(_s0.value) AS word_length,
  COUNT(*) AS n_words,
  COUNT(DISTINCT _s0.value) AS n_unique_words
FROM tpch.customer AS customer
CROSS JOIN LATERAL SPLIT_TO_TABLE(
  TRIM(
    REPLACE(REPLACE(REPLACE(REPLACE(customer.c_comment, ';', ''), ',', ''), ':', ''), '.', ''),
    ' '
  ),
  ' '
) AS _s0
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
