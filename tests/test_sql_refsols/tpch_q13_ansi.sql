WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    o_custkey
  FROM tpch.orders
  WHERE
    NOT o_comment LIKE '%special%requests%'
  GROUP BY
    o_custkey
), _t0 AS (
  SELECT
    COUNT(*) AS custdist,
    COALESCE(_s1.n_rows, 0) AS num_non_special_orders
  FROM tpch.customer AS customer
  LEFT JOIN _s1 AS _s1
    ON _s1.o_custkey = customer.c_custkey
  GROUP BY
    COALESCE(_s1.n_rows, 0)
)
SELECT
  num_non_special_orders AS C_COUNT,
  custdist AS CUSTDIST
FROM _t0
ORDER BY
  custdist DESC,
  num_non_special_orders DESC
LIMIT 10
