WITH _t2 AS (
  SELECT
    nation.n_nationkey,
    supplier.s_phone,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY nation.n_nationkey, SUBSTRING(
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
          ) ORDER BY supplier.s_acctbal DESC) - 1.0
        ) - (
          CAST((
            COUNT(supplier.s_acctbal) OVER (PARTITION BY nation.n_nationkey, SUBSTRING(
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
            )) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN supplier.s_acctbal
      ELSE NULL
    END AS expr_2
  FROM tpch.nation AS nation
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'MIDDLE EAST'
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
), _s5 AS (
  SELECT
    SUBSTRING(
      s_phone,
      CASE WHEN (
        LENGTH(s_phone) + 0
      ) < 1 THEN 1 ELSE (
        LENGTH(s_phone) + 0
      ) END
    ) AS expr_1,
    n_nationkey,
    AVG(expr_2) AS avg_expr2
  FROM _t2
  GROUP BY
    1,
    2
)
SELECT
  customer.c_name AS customer_name,
  ABS(customer.c_acctbal - _s5.avg_expr2) AS delta
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
  2
LIMIT 5
