SELECT
  L_ORDERKEY,
  REVENUE,
  O_ORDERDATE,
  O_SHIPPRIORITY
FROM (
  SELECT
    SUM(REVENUE) AS REVENUE,
    L_ORDERKEY,
    O_ORDERDATE,
    O_SHIPPRIORITY
  FROM (
    SELECT
      L_ORDERKEY,
      O_ORDERDATE,
      O_SHIPPRIORITY,
      REVENUE
    FROM (
      SELECT
        L_EXTENDEDPRICE * (
          1 - L_DISCOUNT
        ) AS REVENUE,
        L_ORDERKEY
      FROM (
        SELECT
          L_DISCOUNT,
          L_EXTENDEDPRICE,
          L_ORDERKEY
        FROM (
          SELECT
            L_DISCOUNT,
            L_EXTENDEDPRICE,
            L_ORDERKEY,
            L_SHIPDATE
          FROM LINEITEM
        )
        WHERE
          L_SHIPDATE > '1995-03-15'
      )
    )
    INNER JOIN (
      SELECT
        O_ORDERDATE,
        O_ORDERKEY,
        O_SHIPPRIORITY
      FROM (
        SELECT
          O_CUSTKEY,
          O_ORDERDATE,
          O_ORDERKEY,
          O_SHIPPRIORITY
        FROM ORDERS
        WHERE
          O_ORDERDATE < '1995-03-15'
      )
      INNER JOIN (
        SELECT
          C_CUSTKEY
        FROM (
          SELECT
            C_CUSTKEY,
            C_MKTSEGMENT
          FROM CUSTOMER
        )
        WHERE
          C_MKTSEGMENT = 'BUILDING'
      )
        ON O_CUSTKEY = C_CUSTKEY
    )
      ON L_ORDERKEY = O_ORDERKEY
  )
  GROUP BY
    L_ORDERKEY,
    O_ORDERDATE,
    O_SHIPPRIORITY
)
ORDER BY
  REVENUE DESC,
  O_ORDERDATE,
  L_ORDERKEY
LIMIT 10
