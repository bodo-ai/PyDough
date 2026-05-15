SELECT
  p_partkey AS "key",
  TRUNC(
    CAST(NVL(SUBSTR(p_brand, CASE WHEN ABS(-2) < LENGTH(p_brand) THEN -2 ELSE 1 END), '') || '' || NVL(SUBSTR(p_brand, 8), '') || '' || NVL(
      SUBSTR(
        p_brand,
        CASE WHEN ABS(-2) < LENGTH(p_brand) THEN -2 ELSE 1 END,
        CASE
          WHEN ABS(-2) > LENGTH(p_brand)
          THEN LENGTH(p_brand) + -1
          ELSE GREATEST(1, 0)
        END
      ),
      ''
    ) AS DOUBLE PRECISION),
    '0'
  ) AS a,
  UPPER(
    LEAST(
      NVL(
        REGEXP_SUBSTR(
          p_name,
          '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
          1,
          CASE
            WHEN (
              (
                LENGTH(p_name) - LENGTH(REPLACE(p_name, ' '))
              ) / LENGTH(' ')
            ) >= 1
            THEN 2
            ELSE NULL
          END,
          NULL,
          1
        ),
        CHR(0)
      ),
      NVL(
        REGEXP_SUBSTR(
          p_name,
          '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
          1,
          CASE
            WHEN (
              (
                LENGTH(p_name) - LENGTH(REPLACE(p_name, ' '))
              ) / LENGTH(' ')
            ) + 1 >= (
              (
                LENGTH(p_name) - LENGTH(REPLACE(p_name, ' '))
              ) / LENGTH(' ')
            ) + 1
            AND (
              (
                LENGTH(p_name) - LENGTH(REPLACE(p_name, ' '))
              ) / LENGTH(' ')
            ) >= 0
            THEN (
              (
                LENGTH(p_name) - LENGTH(REPLACE(p_name, ' '))
              ) / LENGTH(' ')
            ) + 1
            ELSE NULL
          END,
          NULL,
          1
        ),
        CHR(0)
      )
    )
  ) AS b,
  RTRIM(LTRIM(SUBSTR(p_name, 1, 2), 'o'), 'o') AS c,
  LPAD(TO_CHAR(p_size), 3, '0') AS d,
  RPAD(TO_CHAR(p_size), 3, '0') AS e,
  REPLACE(p_mfgr, 'Manufacturer#', 'm') AS f,
  REPLACE(LOWER(p_container), ' ', '') AS g,
  CASE
    WHEN LENGTH('o') IS NULL OR LENGTH(p_name) IS NULL
    THEN 0
    ELSE CAST((
      LENGTH(p_name) - NVL(LENGTH(REPLACE(p_name, 'o', '')), 0)
    ) / LENGTH('o') AS INT)
  END + (
    (
      INSTR(p_name, 'o') - 1
    ) / 100.0
  ) AS h,
  ROUND(POWER(GREATEST(p_size, 10), 0.5), 3) AS i
FROM TPCH.PART
ORDER BY
  1 NULLS FIRST
FETCH FIRST 5 ROWS ONLY
