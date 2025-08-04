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
      CASE
        WHEN CHAR_LENGTH(p_name) = 0
        THEN NULL
        WHEN CHAR_LENGTH(' ') = 0
        THEN CASE WHEN ABS(2) = 1 THEN p_name ELSE NULL END
        WHEN (
          CHAR_LENGTH(p_name) - CHAR_LENGTH(REPLACE(p_name, ' ', ''))
        ) / CHAR_LENGTH(' ') >= 1
        THEN SUBSTRING_INDEX(SUBSTRING_INDEX(p_name, ' ', 2), ' ', -1)
        ELSE NULL
      END,
      CASE
        WHEN CHAR_LENGTH(p_name) = 0
        THEN NULL
        WHEN CHAR_LENGTH(' ') = 0
        THEN CASE WHEN ABS(-1) = 1 THEN p_name ELSE NULL END
        WHEN (
          CHAR_LENGTH(p_name) - CHAR_LENGTH(REPLACE(p_name, ' ', ''))
        ) / CHAR_LENGTH(' ') + 1 >= ABS(-1)
        THEN SUBSTRING_INDEX(SUBSTRING_INDEX(p_name, ' ', -1), ' ', 1)
        ELSE NULL
      END
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
