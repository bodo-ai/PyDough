WITH _s5 AS (
  SELECT DISTINCT
    _s2.val AS cust_word
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
  ) AS _s2(val, idx)
), _u_0 AS (
  SELECT
    cust_word AS _u_1
  FROM _s5
  GROUP BY
    1
)
SELECT
  COUNT(DISTINCT _s0.val) AS n_double_words
FROM tpch.supplier AS supplier
CROSS JOIN LATERAL (
  SELECT
    UNNEST(
      STR_SPLIT(
        TRIM(
          REPLACE(REPLACE(REPLACE(REPLACE(supplier.s_comment, ';', ''), ',', ''), ':', ''), '.', ''),
          ' '
        ),
        ' '
      )
    ) AS _col_0,
    GENERATE_SUBSCRIPTS(
      STR_SPLIT(
        TRIM(
          REPLACE(REPLACE(REPLACE(REPLACE(supplier.s_comment, ';', ''), ',', ''), ':', ''), '.', ''),
          ' '
        ),
        ' '
      ),
      1
    ) - 1 AS _col_1
) AS _s0(val, idx)
LEFT JOIN _u_0 AS _u_0
  ON _s0.val = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
