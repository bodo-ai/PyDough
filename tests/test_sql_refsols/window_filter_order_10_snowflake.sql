WITH _t1 AS (
  SELECT
    1 AS "_"
  FROM tpch.orders
  WHERE
    NOT EXISTS(
      SELECT
        1 AS "1"
      FROM tpch.customer
      WHERE
        c_custkey = orders.o_custkey AND c_mktsegment = 'BUILDING'
    )
    AND o_clerk = 'Clerk#000000001'
  QUALIFY
    o_totalprice < (
      0.05 * AVG(CAST(NULL AS INT)) OVER ()
    )
)
SELECT
  COUNT(*) AS n
FROM _t1
