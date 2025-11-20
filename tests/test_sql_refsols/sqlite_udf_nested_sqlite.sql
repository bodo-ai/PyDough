WITH _s1 AS (
  SELECT
    o_custkey,
    MIN(o_orderdate) AS min_oorderdate,
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    1
), _t2 AS (
  SELECT
    MIN(customer.c_acctbal) OVER () AS min_bal,
    customer.c_acctbal,
    customer.c_mktsegment,
    _s1.min_oorderdate,
    _s1.n_rows
  FROM tpch.customer AS customer
  LEFT JOIN _s1 AS _s1
    ON _s1.o_custkey = customer.c_custkey
)
SELECT
  ROUND(
    CAST((
      100.0 * SUM(
        CASE
          WHEN CASE
            WHEN c_mktsegment = 'BUILDING'
            THEN c_acctbal > 0
            WHEN c_mktsegment = 'MACHINERY'
            THEN ABS(min_bal - c_acctbal) <= 500
            WHEN c_mktsegment = 'HOUSEHOLD'
            THEN CAST(STRFTIME('%j', min_oorderdate) AS INTEGER) = '366'
            ELSE FALSE
          END
          THEN 1
        END
      )
    ) AS REAL) / COUNT(*),
    2
  ) AS p
FROM _t2
WHERE
  n_rows > 0
