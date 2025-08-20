WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    o_custkey
  FROM tpch.ORDERS
  WHERE
    NOT o_comment LIKE '%special%requests%'
  GROUP BY
    2
)
SELECT
  COALESCE(_s1.n_rows, 0) AS C_COUNT,
  COUNT(*) AS CUSTDIST
FROM tpch.CUSTOMER AS CUSTOMER
LEFT JOIN _s1 AS _s1
  ON CUSTOMER.c_custkey = _s1.o_custkey
GROUP BY
  1
ORDER BY
  2 DESC,
  1 DESC
LIMIT 10
