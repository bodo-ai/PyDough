SELECT
  p_partkey AS key,
  CAST(CAST(CASE
    WHEN SUBSTR(p_brand, 8) IS NULL
    OR SUBSTR(
      p_brand,
      CASE WHEN (
        LENGTH(p_brand) + -1
      ) < 1 THEN 1 ELSE (
        LENGTH(p_brand) + -1
      ) END
    ) IS NULL
    OR SUBSTR(
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
    ) IS NULL
    THEN NULL
    ELSE CONCAT_WS(
      '',
      SUBSTR(
        p_brand,
        CASE WHEN (
          LENGTH(p_brand) + -1
        ) < 1 THEN 1 ELSE (
          LENGTH(p_brand) + -1
        ) END
      ),
      SUBSTR(p_brand, 8),
      SUBSTR(
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
    )
  END AS DOUBLE) AS BIGINT) AS a,
  UPPER(
    LEAST(
      CASE
        WHEN (
          CAST(CAST((
            LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
          ) AS DOUBLE) AS BIGINT) + 1
        ) < ABS(2)
        THEN ''
        ELSE SPLIT_PART(p_name, ' ', 2)
      END,
      CASE
        WHEN (
          CAST(CAST((
            LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
          ) AS DOUBLE) AS BIGINT) + 1
        ) < ABS(-1)
        THEN ''
        WHEN (
          CAST(CAST((
            LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
          ) AS DOUBLE) AS BIGINT) + 1
        ) >= ABS(-1)
        THEN SPLIT_PART(
          p_name,
          ' ',
          CAST(CAST((
            LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
          ) AS DOUBLE) AS BIGINT) + 1
        )
        ELSE SPLIT_PART(p_name, ' ', -1)
      END
    )
  ) AS b,
  TRIM('o' FROM SUBSTR(p_name, 1, 2)) AS c,
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
