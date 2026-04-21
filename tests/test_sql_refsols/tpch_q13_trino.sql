WITH _s1 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.orders
  WHERE
    NOT o_comment LIKE '%special%requests%'
  GROUP BY
    1
)
SELECT
  COALESCE(_s1.n_rows, 0) AS C_COUNT,
  COUNT(*) AS CUSTDIST
FROM tpch.customer AS customer
LEFT JOIN _s1 AS _s1
  ON _s1.o_custkey = customer.c_custkey
GROUP BY
  1
ORDER BY
  2 DESC,
  1 DESC
LIMIT 10
