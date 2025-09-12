WITH _u_0 AS (
  SELECT
    (
      42 - (
        customers.c_key
      )
    ) AS _u_1,
    branches.b_key AS _u_2
  FROM crbnk.customers AS customers
  JOIN crbnk.branches AS branches
    ON SUBSTRING(
      SUBSTRING(customers.c_addr, -1) || SUBSTRING(customers.c_addr, 1, LENGTH(customers.c_addr) - 1),
      CASE
        WHEN (
          LENGTH(
            SUBSTRING(customers.c_addr, -1) || SUBSTRING(customers.c_addr, 1, LENGTH(customers.c_addr) - 1)
          ) + -7
        ) < 1
        THEN 1
        ELSE (
          LENGTH(
            SUBSTRING(customers.c_addr, -1) || SUBSTRING(customers.c_addr, 1, LENGTH(customers.c_addr) - 1)
          ) + -7
        )
      END,
      CASE
        WHEN (
          LENGTH(
            SUBSTRING(customers.c_addr, -1) || SUBSTRING(customers.c_addr, 1, LENGTH(customers.c_addr) - 1)
          ) + -5
        ) < 1
        THEN 0
        ELSE (
          LENGTH(
            SUBSTRING(customers.c_addr, -1) || SUBSTRING(customers.c_addr, 1, LENGTH(customers.c_addr) - 1)
          ) + -5
        ) - CASE
          WHEN (
            LENGTH(
              SUBSTRING(customers.c_addr, -1) || SUBSTRING(customers.c_addr, 1, LENGTH(customers.c_addr) - 1)
            ) + -7
          ) < 1
          THEN 1
          ELSE (
            LENGTH(
              SUBSTRING(customers.c_addr, -1) || SUBSTRING(customers.c_addr, 1, LENGTH(customers.c_addr) - 1)
            ) + -7
          )
        END
      END
    ) = SUBSTRING(
      branches.b_addr,
      CASE
        WHEN (
          LENGTH(branches.b_addr) + -7
        ) < 1
        THEN 1
        ELSE (
          LENGTH(branches.b_addr) + -7
        )
      END,
      CASE
        WHEN (
          LENGTH(branches.b_addr) + -5
        ) < 1
        THEN 0
        ELSE (
          LENGTH(branches.b_addr) + -5
        ) - CASE
          WHEN (
            LENGTH(branches.b_addr) + -7
          ) < 1
          THEN 1
          ELSE (
            LENGTH(branches.b_addr) + -7
          )
        END
      END
    )
  GROUP BY
    1,
    2
)
SELECT
  COUNT(*) AS n
FROM crbnk.accounts AS accounts
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = accounts.a_custkey AND _u_0._u_2 = accounts.a_branchkey
WHERE
  NOT _u_0._u_1 IS NULL
