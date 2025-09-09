SELECT
  p_partkey AS key,
  CAST(CONCAT_WS(
    '',
    SUBSTRING(p_brand FROM GREATEST(LENGTH(p_brand) + -1, 1)),
    SUBSTRING(p_brand FROM 8),
    SUBSTRING(p_brand FROM GREATEST(LENGTH(p_brand) + -1, 1) FOR GREATEST(LENGTH(p_brand) + -1 - GREATEST(LENGTH(p_brand) + -2, 0), 0))
  ) AS INT) AS a,
  UPPER(LEAST(SPLIT_PART(p_name, ' ', 2), SPLIT_PART(p_name, ' ', -1))) AS b,
  TRIM('o' FROM SUBSTRING(p_name FROM 1 FOR 2)) AS c,
  LPAD(CAST(p_size AS TEXT), 3, '0') AS d,
  RPAD(CAST(p_size AS TEXT), 3, '0') AS e,
  REPLACE(p_mfgr, 'Manufacturer#', 'm') AS f,
  REPLACE(LOWER(p_container), ' ', '') AS g,
  CASE
    WHEN LENGTH('o') = 0
    THEN 0
    ELSE CAST(CAST((
      LENGTH(p_name) - LENGTH(REPLACE(p_name, 'o', ''))
    ) AS DOUBLE PRECISION) / LENGTH('o') AS BIGINT)
  END + (
    CAST((
      POSITION('o' IN p_name) - 1
    ) AS DOUBLE PRECISION) / 100.0
  ) AS h,
  ROUND(CAST(GREATEST(p_size, 10) ^ 0.5 AS DECIMAL), 3) AS i
FROM tpch.part
ORDER BY
  1 NULLS FIRST
LIMIT 5
