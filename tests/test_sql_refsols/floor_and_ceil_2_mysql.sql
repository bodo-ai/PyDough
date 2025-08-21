SELECT
  ps_suppkey AS supplier_key,
  ps_partkey AS part_key,
  FLOOR(ps_availqty) AS complete_parts,
  CEIL(ps_supplycost * FLOOR(ps_availqty)) AS total_cost
FROM tpch.PARTSUPP
ORDER BY
  4 DESC
LIMIT 10
