SELECT
  ps_suppkey AS supplier_key,
  ps_partkey AS part_key,
  FLOOR(ps_availqty) AS complete_parts,
  CEIL(ps_supplycost * FLOOR(ps_availqty)) AS total_cost
FROM tpch_s3_pq.tpch_sf1.partsupp
ORDER BY
  4 DESC
LIMIT 10
