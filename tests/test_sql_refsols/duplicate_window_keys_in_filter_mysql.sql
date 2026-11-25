WITH _t AS (
  SELECT
    o_orderkey,
    o_totalprice,
    ROW_NUMBER() OVER (
      ORDER BY CASE WHEN o_totalprice IS NULL THEN 1 ELSE 0 END DESC, o_totalprice DESC, CASE WHEN o_orderkey IS NULL THEN 1 ELSE 0 END, o_orderkey
    ) AS _w
  FROM tpch.ORDERS
)
SELECT
  o_orderkey AS `key`,
  o_totalprice AS total_price
FROM _t
WHERE
  _w <= 3
ORDER BY
  2 DESC
