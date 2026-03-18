SELECT
  p_partkey AS key,
  CAST(CONCAT_WS(
    ''[0],
    SUBSTRING(
      p_brand,
      CASE WHEN (
        LENGTH(p_brand) + -1
      ) < 1 THEN 1 ELSE (
        LENGTH(p_brand) + -1
      ) END
    )[0],
    SUBSTRING(p_brand, 8)[0],
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
    )[0]
  ) AS BIGINT) AS a,
  UPPER(
    LEAST(
      CASE
        WHEN -CASE
          WHEN LENGTH(' ') = 0
          THEN 0
          ELSE CAST(CAST((
            LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
          ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
        END > 2
        THEN NULL
        WHEN CASE
          WHEN LENGTH(' ') = 0
          THEN 0
          ELSE CAST(CAST((
            LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
          ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
        END < 2
        THEN NULL
        ELSE SPLIT_PART(p_name, ' ', 2)
      END,
      SPLIT_PART(
        p_name,
        ' ',
        CASE
          WHEN LENGTH(' ') = 0
          THEN 0
          ELSE CAST(CAST((
            LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
          ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
        END - -1 + 1
      )
    )
  ) AS b,
  TRIM('o' FROM SUBSTRING(p_name, 1, 2)) AS c,
  LPAD(CAST(p_size AS VARCHAR), 3, '0') AS d,
  RPAD(CAST(p_size AS VARCHAR), 3, '0') AS e,
  REPLACE(p_mfgr, 'Manufacturer#', 'm') AS f,
  REPLACE(LOWER(p_container), ' ', '') AS g,
  CASE
    WHEN LENGTH('o') = 0
    THEN 0
    ELSE CAST(CAST((
      LENGTH(p_name) - LENGTH(REPLACE(p_name, 'o', ''))
    ) AS DOUBLE) / LENGTH('o') AS BIGINT)
  END + (
    CAST((
      STRPOS(p_name, 'o') - 1
    ) AS DOUBLE) / 100.0
  ) AS h,
  ROUND(POWER(GREATEST(p_size, 10), 0.5), 3) AS i
FROM tpch.part
ORDER BY
  1 NULLS FIRST
LIMIT 5
