WITH _t AS (
  SELECT
    o_totalprice,
    AVG(CAST(NULL AS INTEGER)) OVER () AS _w
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
)
SELECT
  COUNT(*) AS n
FROM _t
WHERE
  o_totalprice < (
    0.05 * _w
  )
