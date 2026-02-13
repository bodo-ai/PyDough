SELECT
  ps_suppkey AS supplier_key,
  ps_partkey AS part_key,
  CAST(ps_availqty AS INTEGER) - CASE WHEN ps_availqty < CAST(ps_availqty AS INTEGER) THEN 1 ELSE 0 END AS complete_parts,
  CAST(ps_supplycost * (
    CAST(ps_availqty AS INTEGER) - CASE WHEN ps_availqty < CAST(ps_availqty AS INTEGER) THEN 1 ELSE 0 END
  ) AS INTEGER) + CASE
    WHEN CAST(ps_supplycost * (
      CAST(ps_availqty AS INTEGER) - CASE WHEN ps_availqty < CAST(ps_availqty AS INTEGER) THEN 1 ELSE 0 END
    ) AS INTEGER) < ps_supplycost * (
      CAST(ps_availqty AS INTEGER) - CASE WHEN ps_availqty < CAST(ps_availqty AS INTEGER) THEN 1 ELSE 0 END
    )
    THEN 1
    ELSE 0
  END AS total_cost
FROM tpch.partsupp
ORDER BY
  4 DESC
LIMIT 10
