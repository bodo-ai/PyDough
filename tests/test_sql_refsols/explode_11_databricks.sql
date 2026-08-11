WITH _s3 AS (
  SELECT DISTINCT
    _s1.val AS cust_word
  FROM tpch.customer AS customer
  CROSS JOIN LATERAL POSEXPLODE(
    SPLIT(
      TRIM(' ' FROM REPLACE(REPLACE(REPLACE(REPLACE(customer.c_comment, ';', ''), ',', ''), ':', ''), '.', '')),
      '\\Q \\E'
    )
  ) AS _s1(idx, val)
), _u_0 AS (
  SELECT
    cust_word AS _u_1
  FROM _s3
  GROUP BY
    1
)
SELECT
  COUNT(DISTINCT _s0.val) AS n_double_words
FROM tpch.supplier AS supplier
CROSS JOIN LATERAL POSEXPLODE(
  SPLIT(
    TRIM(' ' FROM REPLACE(REPLACE(REPLACE(REPLACE(supplier.s_comment, ';', ''), ',', ''), ':', ''), '.', '')),
    '\\Q \\E'
  )
) AS _s0(idx, val)
LEFT JOIN _u_0 AS _u_0
  ON _s0.val = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
