WITH _q_0 AS (
  SELECT
    1990 AS `1990`
  UNION ALL
  SELECT
    1991 AS `1991`
  UNION ALL
  SELECT
    1992 AS `1992`
  UNION ALL
  SELECT
    1993 AS `1993`
  UNION ALL
  SELECT
    1994 AS `1994`
  UNION ALL
  SELECT
    1995 AS `1995`
  UNION ALL
  SELECT
    1996 AS `1996`
  UNION ALL
  SELECT
    1997 AS `1997`
  UNION ALL
  SELECT
    1998 AS `1998`
  UNION ALL
  SELECT
    1999 AS `1999`
  UNION ALL
  SELECT
    2000 AS `2000`
), _s5 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) AS year_o_orderdate,
    COUNT(DISTINCT ORDERS.o_custkey) AS ndistinct_o_custkey
  FROM tpch.ORDERS AS ORDERS
  JOIN tpch.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_custkey = ORDERS.o_custkey AND CUSTOMER.c_mktsegment = 'AUTOMOBILE'
  JOIN tpch.NATION AS NATION
    ON CUSTOMER.c_nationkey = NATION.n_nationkey AND NATION.n_name = 'JAPAN'
  WHERE
    ORDERS.o_clerk = 'Clerk#000000925'
  GROUP BY
    1
)
SELECT
  _q_0.`1990` AS year,
  COALESCE(_s5.ndistinct_o_custkey, 0) AS n_orders
FROM _q_0 AS _q_0
LEFT JOIN _s5 AS _s5
  ON _q_0.`1990` = _s5.year_o_orderdate
ORDER BY
  1
