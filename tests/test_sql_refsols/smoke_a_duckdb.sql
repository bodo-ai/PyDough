SELECT
  p_partkey AS key,
  CAST(TRUNC(
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
    ) AS DOUBLE)
  ) AS BIGINT) AS a,
  UPPER(
    CASE
      WHEN SPLIT_PART(p_name, ' ', -1) >= SPLIT_PART(p_name, ' ', 2)
      THEN SPLIT_PART(p_name, ' ', 2)
      WHEN SPLIT_PART(p_name, ' ', -1) <= SPLIT_PART(p_name, ' ', 2)
      THEN SPLIT_PART(p_name, ' ', -1)
    END
  ) AS b,
  TRIM(SUBSTRING(p_name, 1, 2), 'o') AS c,
  CASE
    WHEN LENGTH(CAST(p_size AS TEXT)) >= 3
    THEN SUBSTRING(CAST(p_size AS TEXT), 1, 3)
    ELSE SUBSTRING(CONCAT('000', CAST(p_size AS TEXT)), -3)
  END AS d,
  SUBSTRING(CONCAT(CAST(p_size AS TEXT), '000'), 1, 3) AS e,
  REPLACE(p_mfgr, 'Manufacturer#', 'm') AS f,
  REPLACE(LOWER(p_container), ' ', '') AS g,
  CAST((
    LENGTH(p_name) - LENGTH(REPLACE(p_name, 'o', ''))
  ) AS BIGINT) + (
    (
      STRPOS(p_name, 'o') - 1
    ) / 100.0
  ) AS h,
  ROUND(POWER(CASE WHEN p_size >= 10 THEN p_size WHEN p_size <= 10 THEN 10 END, 0.5), 3) AS i
FROM tpch.part
ORDER BY
  1 NULLS FIRST
LIMIT 5
