SELECT
  LENGTH(_s0.val) AS word_length,
  COUNT(*) AS n_words,
  COUNT(DISTINCT _s0.val) AS n_unique_words
FROM tpch.customer AS customer
CROSS JOIN LATERAL (
  SELECT
    UNNEST(
      STR_SPLIT(
        TRIM(
          REPLACE(REPLACE(REPLACE(REPLACE(customer.c_comment, ';', ''), ',', ''), ':', ''), '.', ''),
          ' '
        ),
        ' '
      )
    ) AS _col_0,
    GENERATE_SUBSCRIPTS(
      STR_SPLIT(
        TRIM(
          REPLACE(REPLACE(REPLACE(REPLACE(customer.c_comment, ';', ''), ',', ''), ':', ''), '.', ''),
          ' '
        ),
        ' '
      ),
      1
    ) - 1 AS _col_1
) AS _s0(val, idx)
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
