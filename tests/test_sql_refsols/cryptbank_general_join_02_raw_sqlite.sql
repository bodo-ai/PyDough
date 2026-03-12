SELECT
  COUNT(*) AS n
FROM crbnk.accounts
WHERE
  EXISTS(
    SELECT
      1 AS "1"
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
    WHERE
      accounts.a_branchkey = branches.b_key
      AND accounts.a_custkey = (
        42 - customers.c_key
      )
  )
