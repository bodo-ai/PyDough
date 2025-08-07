WITH _s5 AS (
  SELECT
    SUBSTRING(
      supplier.s_phone,
      CASE
        WHEN (
          LENGTH(supplier.s_phone) + 0
        ) < 1
        THEN 1
        ELSE (
          LENGTH(supplier.s_phone) + 0
        )
      END
    ) AS expr_1,
    MEDIAN(supplier.s_acctbal) AS median_s_acctbal,
    nation.n_nationkey
  FROM tpch.nation AS nation
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'MIDDLE EAST'
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
  GROUP BY
    SUBSTRING(
      supplier.s_phone,
      CASE
        WHEN (
          LENGTH(supplier.s_phone) + 0
        ) < 1
        THEN 1
        ELSE (
          LENGTH(supplier.s_phone) + 0
        )
      END
    ),
    nation.n_nationkey
)
SELECT
  customer.c_name AS customer_name,
  ABS(customer.c_acctbal - _s5.median_s_acctbal) AS delta
FROM tpch.customer AS customer
JOIN _s5 AS _s5
  ON _s5.expr_1 = SUBSTRING(
    customer.c_phone,
    CASE
      WHEN (
        LENGTH(customer.c_phone) + 0
      ) < 1
      THEN 1
      ELSE (
        LENGTH(customer.c_phone) + 0
      )
    END
  )
  AND _s5.n_nationkey = customer.c_nationkey
WHERE
  customer.c_mktsegment = 'AUTOMOBILE'
ORDER BY
  ABS(customer.c_acctbal - _s5.median_s_acctbal)
LIMIT 5
