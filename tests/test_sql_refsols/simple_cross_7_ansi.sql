WITH _s3 AS (
  SELECT
    part.p_partkey,
    COUNT(*) AS n_rows
  FROM tpch.part AS part
  JOIN tpch.part AS part_2
    ON ABS(part_2.p_retailprice - part.p_retailprice) < 5.0
    AND part.p_brand = part_2.p_brand
    AND part.p_mfgr = part_2.p_mfgr
    AND part.p_partkey < part_2.p_partkey
    AND part_2.p_name LIKE '%tomato%'
  WHERE
    part.p_brand = 'Brand#35'
    AND part.p_mfgr = 'Manufacturer#3'
    AND part.p_name LIKE '%tomato%'
  GROUP BY
    1
)
SELECT
  part.p_partkey AS original_part_key,
  COALESCE(_s3.n_rows, 0) AS n_other_parts
FROM tpch.part AS part
LEFT JOIN _s3 AS _s3
  ON _s3.p_partkey = part.p_partkey
WHERE
  part.p_brand = 'Brand#35'
  AND part.p_mfgr = 'Manufacturer#3'
  AND part.p_name LIKE '%tomato%'
ORDER BY
  2 DESC,
  1
LIMIT 5
