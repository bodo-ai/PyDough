WITH _s0 AS (
  SELECT
    b_addr,
    b_key
  FROM crbnk.branches
), _s1 AS (
  SELECT
    c_addr,
    c_key
  FROM crbnk.customers
), _s7 AS (
  SELECT
    _s2.b_key,
    _s3.c_key,
    COUNT(*) AS n_rows
  FROM _s0 AS _s2
  JOIN _s1 AS _s3
    ON SUBSTRING(
      _s2.b_addr,
      CASE
        WHEN (
          LENGTH(_s2.b_addr) + -7
        ) < 1
        THEN 1
        ELSE (
          LENGTH(_s2.b_addr) + -7
        )
      END,
      CASE
        WHEN (
          LENGTH(_s2.b_addr) + -5
        ) < 1
        THEN 0
        ELSE (
          LENGTH(_s2.b_addr) + -5
        ) - CASE
          WHEN (
            LENGTH(_s2.b_addr) + -7
          ) < 1
          THEN 1
          ELSE (
            LENGTH(_s2.b_addr) + -7
          )
        END
      END
    ) = SUBSTRING(
      _s3.c_addr,
      CASE
        WHEN (
          LENGTH(_s3.c_addr) + -7
        ) < 1
        THEN 1
        ELSE (
          LENGTH(_s3.c_addr) + -7
        )
      END,
      CASE
        WHEN (
          LENGTH(_s3.c_addr) + -5
        ) < 1
        THEN 0
        ELSE (
          LENGTH(_s3.c_addr) + -5
        ) - CASE
          WHEN (
            LENGTH(_s3.c_addr) + -7
          ) < 1
          THEN 1
          ELSE (
            LENGTH(_s3.c_addr) + -7
          )
        END
      END
    )
  JOIN crbnk.accounts AS accounts
    ON _s2.b_key = accounts.a_branchkey AND _s3.c_key = accounts.a_custkey
  GROUP BY
    1,
    2
)
SELECT
  _s0.b_key AS branch_key,
  COUNT(*) AS n_local_cust,
  COALESCE(SUM(_s7.n_rows), 0) AS n_local_cust_local_acct
FROM _s0 AS _s0
JOIN _s1 AS _s1
  ON SUBSTRING(
    _s0.b_addr,
    CASE
      WHEN (
        LENGTH(_s0.b_addr) + -7
      ) < 1
      THEN 1
      ELSE (
        LENGTH(_s0.b_addr) + -7
      )
    END,
    CASE
      WHEN (
        LENGTH(_s0.b_addr) + -5
      ) < 1
      THEN 0
      ELSE (
        LENGTH(_s0.b_addr) + -5
      ) - CASE
        WHEN (
          LENGTH(_s0.b_addr) + -7
        ) < 1
        THEN 1
        ELSE (
          LENGTH(_s0.b_addr) + -7
        )
      END
    END
  ) = SUBSTRING(
    _s1.c_addr,
    CASE
      WHEN (
        LENGTH(_s1.c_addr) + -7
      ) < 1
      THEN 1
      ELSE (
        LENGTH(_s1.c_addr) + -7
      )
    END,
    CASE
      WHEN (
        LENGTH(_s1.c_addr) + -5
      ) < 1
      THEN 0
      ELSE (
        LENGTH(_s1.c_addr) + -5
      ) - CASE
        WHEN (
          LENGTH(_s1.c_addr) + -7
        ) < 1
        THEN 1
        ELSE (
          LENGTH(_s1.c_addr) + -7
        )
      END
    END
  )
LEFT JOIN _s7 AS _s7
  ON _s0.b_key = _s7.b_key AND _s1.c_key = _s7.c_key
GROUP BY
  1
