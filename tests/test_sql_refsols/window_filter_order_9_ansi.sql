WITH _t1 AS (
  SELECT
    1 AS "_"
  FROM tpch.orders
  WHERE
    o_clerk = 'Clerk#000000001'
  QUALIFY
    expr_0 IS NULL AND o_totalprice < (
      0.05 * AVG(total_spent) OVER ()
    )
)
SELECT
  COUNT(*) AS n
FROM _t1
