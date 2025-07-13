WITH _t0 AS (
  SELECT
    CEIL(ps_supplycost * FLOOR(ps_availqty)) AS total_cost_1,
    ps_availqty,
    ps_partkey,
    ps_suppkey
  FROM tpch.partsupp
  ORDER BY
    CEIL(ps_supplycost * FLOOR(ps_availqty)) DESC
  LIMIT 10
)
SELECT
  ps_suppkey AS supplier_key,
  ps_partkey AS part_key,
  FLOOR(ps_availqty) AS complete_parts,
  total_cost_1 AS total_cost
FROM _t0
ORDER BY
  total_cost_1 DESC
