SELECT
  ps_suppkey AS supplier_key,
  ps_partkey AS part_key,
  FLOOR(ps_availqty) AS complete_parts,
  CEIL(ps_supplycost * FLOOR(ps_availqty)) AS total_cost
FROM TPCH.PARTSUPP
ORDER BY
  CEIL(ps_supplycost * FLOOR(ps_availqty)) DESC NULLS LAST
LIMIT 10
