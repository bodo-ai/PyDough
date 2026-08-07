WITH _s3 AS (
  SELECT DISTINCT
    _s1.value AS cust_word
  FROM tpch.customer AS customer
  CROSS JOIN LATERAL SPLIT_TO_TABLE(
    TRIM(
      REPLACE(REPLACE(REPLACE(REPLACE(customer.c_comment, ';', ''), ',', ''), ':', ''), '.', ''),
      ' '
    ),
    ' '
  ) AS _s1
), _u_0 AS (
  SELECT
    cust_word AS _u_1
  FROM _s3
  GROUP BY
    1
)
SELECT
  COUNT(DISTINCT _s0.value) AS n_double_words
FROM tpch.supplier AS supplier
CROSS JOIN LATERAL SPLIT_TO_TABLE(
  TRIM(
    REPLACE(REPLACE(REPLACE(REPLACE(supplier.s_comment, ';', ''), ',', ''), ':', ''), '.', ''),
    ' '
  ),
  ' '
) AS _s0
LEFT JOIN _u_0 AS _u_0
  ON _s0.value = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
