WITH _t AS (
  SELECT
    o_orderkey,
    o_totalprice,
    ROW_NUMBER() OVER (ORDER BY o_totalprice DESC NULLS FIRST, o_orderkey) AS _w
  FROM mysql.tpch.orders
)
SELECT
  o_orderkey AS key,
  o_totalprice AS total_price
FROM _t
WHERE
  _w <= 3
ORDER BY
  2 DESC
