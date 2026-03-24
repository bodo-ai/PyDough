SELECT
  p_partkey AS key,
  CAST(CONCAT_WS(
    '',
    SUBSTRING(
      p_brand,
      CASE WHEN (
        LENGTH(p_brand) + -1
      ) < 1 THEN 1 ELSE (
        LENGTH(p_brand) + -1
      ) END
    ),
    SUBSTRING(p_brand, 8),
    SUBSTRING(
      p_brand,
      CASE WHEN (
        LENGTH(p_brand) + -1
      ) < 1 THEN 1 ELSE (
        LENGTH(p_brand) + -1
      ) END,
      CASE
        WHEN (
          LENGTH(p_brand) + 0
        ) < 1
        THEN 0
        ELSE (
          LENGTH(p_brand) + 0
        ) - CASE WHEN (
          LENGTH(p_brand) + -1
        ) < 1 THEN 1 ELSE (
          LENGTH(p_brand) + -1
        ) END
      END
    )
  ) AS BIGINT) AS a,
  UPPER(
    LEAST(
      CASE
        WHEN -CAST(CAST((
          LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
        ) AS DOUBLE) AS BIGINT) > 2
        THEN NULL
        WHEN CAST(CAST((
          LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
        ) AS DOUBLE) AS BIGINT) < 2
        THEN NULL
        ELSE SPLIT_PART(p_name, ' ', 2)
      END,
      SPLIT_PART(
        p_name,
        ' ',
        CAST(CAST((
          LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
        ) AS DOUBLE) AS BIGINT) - -1 + 1
      )
    )
  ) AS b,
  TRIM('o' FROM SUBSTRING(p_name, 1, 2)) AS c,
  LPAD(CAST(p_size AS VARCHAR), 3, '0') AS d,
  RPAD(CAST(p_size AS VARCHAR), 3, '0') AS e,
  REPLACE(p_mfgr, 'Manufacturer#', 'm') AS f,
  REPLACE(LOWER(p_container), ' ', '') AS g,
  CAST(CAST((
    LENGTH(p_name) - LENGTH(REPLACE(p_name, 'o', ''))
  ) AS DOUBLE) AS BIGINT) + (
    CAST((
      STRPOS(p_name, 'o') - 1
    ) AS DOUBLE) / 100.0
  ) AS h,
  ROUND(POWER(GREATEST(p_size, 10), 0.5), 3) AS i
FROM tpch.part
ORDER BY
  1 NULLS FIRST
LIMIT 5
