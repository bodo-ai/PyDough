WITH _t0 AS (
  SELECT
    ps_availqty,
    ps_partkey,
    ps_suppkey,
    CEIL(ps_supplycost * FLOOR(ps_availqty)) AS total_cost
  FROM tpch.partsupp
  ORDER BY
    total_cost DESC
  LIMIT 10
)
SELECT
  ps_suppkey AS supplier_key,
  ps_partkey AS part_key,
  FLOOR(ps_availqty) AS complete_parts,
  total_cost
FROM _t0
ORDER BY
  total_cost DESC
