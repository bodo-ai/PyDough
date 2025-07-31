WITH _s2 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    p_name AS rest,
    ' ' AS delim,
    2 AS idx
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN LOCATE(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, LOCATE(delim, rest) - 1)
    END AS part,
    CASE
      WHEN LOCATE(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, LOCATE(delim, rest) + CHAR_LENGTH(delim))
    END AS rest,
    delim,
    idx
  FROM _s2
  WHERE
    rest <> ''
), _s3 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s2
), _s0 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    p_name AS rest,
    ' ' AS delim,
    -1 AS idx
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN LOCATE(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, LOCATE(delim, rest) - 1)
    END AS part,
    CASE
      WHEN LOCATE(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, LOCATE(delim, rest) + CHAR_LENGTH(delim))
    END AS rest,
    delim,
    idx
  FROM _s0
  WHERE
    rest <> ''
), _s1 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s0
)
SELECT
  p_partkey AS `key`,
  CAST(CONCAT_WS(
    '',
    SUBSTRING(p_brand, -2),
    SUBSTRING(p_brand, 8),
    SUBSTRING(p_brand, -2, GREATEST(1, 0))
  ) AS SIGNED) AS a,
  UPPER(
    LEAST(
      (
        SELECT
          _s2.part
        FROM _s2 AS _s2
        CROSS JOIN _s3 AS _s3
        WHERE
          _s2.part_index <> 0
          AND _s2.part_index = CASE
            WHEN _s2.idx > 0
            THEN _s2.idx
            WHEN _s2.idx < 0
            THEN _s3.total_parts + _s2.idx + 1
            ELSE 1
          END
      ),
      (
        SELECT
          _s0.part
        FROM _s0 AS _s0
        CROSS JOIN _s1 AS _s1
        WHERE
          _s0.part_index <> 0
          AND _s0.part_index = CASE
            WHEN _s0.idx > 0
            THEN _s0.idx
            WHEN _s0.idx < 0
            THEN _s1.total_parts + _s0.idx + 1
            ELSE 1
          END
      )
    )
  ) AS b,
  TRIM('o' FROM SUBSTRING(p_name, 1, 2)) AS c,
  LPAD(CAST(p_size AS CHAR), 3, '0') AS d,
  RPAD(CAST(p_size AS CHAR), 3, '0') AS e,
  REPLACE(p_mfgr, 'Manufacturer#', 'm') AS f,
  REPLACE(LOWER(p_container), ' ', '') AS g,
  CASE
    WHEN CHAR_LENGTH('o') = 0
    THEN 0
    ELSE CAST(CHAR_LENGTH(p_name) - CHAR_LENGTH(REPLACE(p_name, 'o', '')) / CHAR_LENGTH('o') AS SIGNED)
  END + (
    (
      LOCATE('o', p_name) - 1
    ) / 100.0
  ) AS h,
  ROUND(POWER(GREATEST(p_size, 10), 0.5), 3) AS i
FROM tpch.PART
ORDER BY
  p_partkey
LIMIT 5
