WITH _s3 AS (
  SELECT DISTINCT
    partsupp.ps_suppkey
  FROM tpch.partsupp AS partsupp
  JOIN tpch.part AS part
    ON part.p_partkey = partsupp.ps_partkey
), _s7 AS (
  SELECT DISTINCT
    ps_suppkey
  FROM tpch.partsupp
  WHERE
    NOT _u_0._u_1 IS NULL
)
SELECT
  COUNT(*) AS n
FROM tpch.supplier AS supplier
JOIN _s3 AS _s3
  ON _s3.ps_suppkey = supplier.s_suppkey
JOIN _s7 AS _s7
  ON _s7.ps_suppkey = supplier.s_suppkey
WHERE
  supplier.s_nationkey <= 3
