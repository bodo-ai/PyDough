SELECT
  LENGTH(_s0.val) AS word_length,
  COUNT(*) AS n_words,
  COUNT(DISTINCT _s0.val) AS n_unique_words
FROM tpch.customer AS customer
CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(
  TRIM(' ' FROM REPLACE(REPLACE(REPLACE(REPLACE(customer.c_comment, ';', ''), ',', ''), ':', ''), '.', '')),
  ' '
)) WITH ORDINALITY AS _s0(val, idx)
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
