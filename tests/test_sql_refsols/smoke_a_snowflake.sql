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
  UPPER(LEAST(SPLIT_PART(p_name, ' ', 2), SPLIT_PART(p_name, ' ', -1))) AS b,
  TRIM(SUBSTRING(p_name, 1, 2), 'o') AS c,
  LPAD(CAST(p_size AS TEXT), 3, '0') AS d,
  RPAD(CAST(p_size AS TEXT), 3, '0') AS e,
  REPLACE(p_mfgr, 'Manufacturer#', 'm') AS f,
  REPLACE(LOWER(p_container), ' ', '') AS g,
  CASE
    WHEN LENGTH('o') = 0
    THEN 0
    ELSE CAST(LENGTH(p_name) - LENGTH(REPLACE(p_name, 'o', '')) / LENGTH('o') AS BIGINT)
  END + (
    (
      CHARINDEX('o', p_name) - 1
    ) / 100.0
  ) AS h,
  ROUND(POWER(GREATEST(p_size, 10), 0.5), 3) AS i
FROM TPCH.PART
ORDER BY
  p_partkey NULLS FIRST
LIMIT 5
