SELECT
  CONCAT_WS(' - ', customer.c_mktsegment, nation.n_name) AS label,
  COUNT(*) AS n
FROM tpch.customer AS customer
JOIN tpch.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
GROUP BY
  1
ORDER BY
  1
