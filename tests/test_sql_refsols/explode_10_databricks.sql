SELECT
  LENGTH(_s0.val) AS word_length,
  COUNT(*) AS n_words,
  COUNT(DISTINCT _s0.val) AS n_unique_words
FROM tpch.customer AS customer
CROSS JOIN LATERAL POSEXPLODE(
  SPLIT(
    TRIM(' ' FROM REPLACE(REPLACE(REPLACE(REPLACE(customer.c_comment, ';', ''), ',', ''), ':', ''), '.', '')),
    '\\Q \\E'
  )
) AS _s0(idx, val)
GROUP BY
  1
ORDER BY
  1
