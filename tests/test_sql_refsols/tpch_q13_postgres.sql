WITH _t1 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM tpch.orders
  WHERE
    NOT o_comment LIKE '%special%requests%'
  GROUP BY
    o_custkey
)
SELECT
  n_rows AS C_COUNT,
  COUNT(*) AS CUSTDIST
FROM _t1
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST,
  1 DESC NULLS LAST
LIMIT 10
