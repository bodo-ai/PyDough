WITH _t0 AS (
  SELECT
    o_custkey,
    o_orderkey
  FROM main.orders
  WHERE
    o_custkey <= 2
  ORDER BY
    1 NULLS FIRST,
    2 NULLS FIRST
  LIMIT 5
)
SELECT
  o_orderkey AS oid,
  o_custkey AS c1,
  o_custkey AS c2
FROM _t0
ORDER BY
  1 DESC
