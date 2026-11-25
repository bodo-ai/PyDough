WITH _t0 AS (
  SELECT
    o_custkey,
    o_orderkey
  FROM snowflake_sample_data.tpch_sf1.orders
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
  1 DESC NULLS LAST
