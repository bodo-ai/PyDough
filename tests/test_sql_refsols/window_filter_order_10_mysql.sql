WITH _t AS (
  SELECT
    o_totalprice,
    AVG(CAST(NULL AS SIGNED)) OVER () AS _w
  FROM tpch.ORDERS
  WHERE
    NOT EXISTS(
      SELECT
        1 AS `1`
      FROM tpch.CUSTOMER
      WHERE
        c_custkey = ORDERS.o_custkey AND c_mktsegment = 'BUILDING'
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
