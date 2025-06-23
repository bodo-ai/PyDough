WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    o_custkey
  FROM tpch.orders
  WHERE
    NOT o_comment LIKE '%special%requests%'
  GROUP BY
    o_custkey
)
SELECT
  COALESCE(_s1.n_rows, 0) AS C_COUNT,
  COUNT(*) AS CUSTDIST
FROM tpch.customer AS customer
LEFT JOIN _s1 AS _s1
  ON _s1.o_custkey = customer.c_custkey
GROUP BY
  COALESCE(_s1.n_rows, 0)
ORDER BY
  custdist DESC,
  COALESCE(_s1.n_rows, 0) DESC
LIMIT 10
