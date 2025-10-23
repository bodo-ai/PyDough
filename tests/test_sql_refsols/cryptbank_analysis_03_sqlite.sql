WITH _s0 AS (
  SELECT
    a_custkey,
    a_key
  FROM crbnk.accounts
), _s1 AS (
  SELECT
    t_amount,
    t_destaccount,
    t_sourceaccount,
    t_ts
  FROM crbnk.transactions
), _s2 AS (
  SELECT
    a_branchkey,
    a_key
  FROM crbnk.accounts
), _t4 AS (
  SELECT
    b_addr,
    b_key
  FROM crbnk.branches
  WHERE
    SUBSTRING(
      b_addr,
      CASE WHEN (
        LENGTH(b_addr) + -4
      ) < 1 THEN 1 ELSE (
        LENGTH(b_addr) + -4
      ) END
    ) = '94105'
), _t AS (
  SELECT
    _s0.a_custkey,
    _s1.t_amount,
    ROW_NUMBER() OVER (PARTITION BY _s1.t_sourceaccount ORDER BY DATETIME(_s1.t_ts, '+54321 seconds')) AS _w
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s1.t_sourceaccount = CASE
      WHEN _s0.a_key = 0
      THEN 0
      ELSE CASE WHEN _s0.a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(_s0.a_key, 1 + INSTR(_s0.a_key, '-'), CAST(LENGTH(_s0.a_key) AS REAL) / 2) AS INTEGER)
    END
  JOIN _s2 AS _s2
    ON _s1.t_destaccount = CASE
      WHEN _s2.a_key = 0
      THEN 0
      ELSE CASE WHEN _s2.a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(_s2.a_key, 1 + INSTR(_s2.a_key, '-'), CAST(LENGTH(_s2.a_key) AS REAL) / 2) AS INTEGER)
    END
  JOIN _t4 AS _t4
    ON _s2.a_branchkey = _t4.b_key
), _s7 AS (
  SELECT
    a_custkey,
    SUM((
      1025.67 - t_amount
    )) AS sum_unmask_t_amount
  FROM _t
  WHERE
    _w = 1
  GROUP BY
    1
), _t_2 AS (
  SELECT
    _s8.a_custkey,
    _s9.t_amount,
    ROW_NUMBER() OVER (PARTITION BY _s9.t_destaccount ORDER BY DATETIME(_s9.t_ts, '+54321 seconds')) AS _w
  FROM _s0 AS _s8
  JOIN _s1 AS _s9
    ON _s9.t_destaccount = CASE
      WHEN _s8.a_key = 0
      THEN 0
      ELSE CASE WHEN _s8.a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(_s8.a_key, 1 + INSTR(_s8.a_key, '-'), CAST(LENGTH(_s8.a_key) AS REAL) / 2) AS INTEGER)
    END
  JOIN _s2 AS _s10
    ON _s9.t_sourceaccount = CASE
      WHEN _s10.a_key = 0
      THEN 0
      ELSE CASE WHEN _s10.a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(_s10.a_key, 1 + INSTR(_s10.a_key, '-'), CAST(LENGTH(_s10.a_key) AS REAL) / 2) AS INTEGER)
    END
  JOIN _t4 AS _t8
    ON _s10.a_branchkey = _t8.b_key
), _s15 AS (
  SELECT
    a_custkey,
    SUM((
      1025.67 - t_amount
    )) AS sum_unmask_t_amount
  FROM _t_2
  WHERE
    _w = 1
  GROUP BY
    1
)
SELECT
  42 - customers.c_key AS key,
  CONCAT_WS(' ', LOWER(customers.c_fname), LOWER(customers.c_lname)) AS name,
  COALESCE(_s7.sum_unmask_t_amount, 0) AS first_sends,
  COALESCE(_s15.sum_unmask_t_amount, 0) AS first_recvs
FROM crbnk.customers AS customers
<<<<<<< HEAD
LEFT JOIN _s7 AS _s7
  ON _s7.a_custkey = (
    42 - customers.c_key
  )
LEFT JOIN _s15 AS _s15
  ON _s15.a_custkey = (
    42 - customers.c_key
  )
=======
JOIN _s7 AS _s7
  ON _s7.a_custkey = customers.c_key
JOIN _s15 AS _s15
  ON _s15.a_custkey = customers.c_key
>>>>>>> main
ORDER BY
  COALESCE(_s7.sum_unmask_t_amount, 0) + COALESCE(_s15.sum_unmask_t_amount, 0) DESC,
  1
LIMIT 3
