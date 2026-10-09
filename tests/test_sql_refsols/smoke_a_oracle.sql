SELECT
  P_PARTKEY AS "key",
  TRUNC(
    CAST(NVL(SUBSTR(P_BRAND, CASE WHEN ABS(-2) < LENGTH(P_BRAND) THEN -2 ELSE 1 END), '') || '' || NVL(SUBSTR(P_BRAND, 8), '') || '' || NVL(
      SUBSTR(
        P_BRAND,
        CASE WHEN ABS(-2) < LENGTH(P_BRAND) THEN -2 ELSE 1 END,
        CASE
          WHEN ABS(-2) > LENGTH(P_BRAND)
          THEN LENGTH(P_BRAND) + -1
          ELSE GREATEST(1, 0)
        END
      ),
      ''
    ) AS DOUBLE PRECISION),
    0
  ) AS a,
  UPPER(
    LEAST(
      NVL(
        REGEXP_SUBSTR(
          P_NAME,
          '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
          1,
          CASE
            WHEN (
              LENGTH(P_NAME) - LENGTH(REPLACE(P_NAME, ' '))
            ) >= 1
            THEN 2
            ELSE NULL
          END,
          NULL,
          1
        ),
        CHR(  0)
      ),
      NVL(
        REGEXP_SUBSTR(
          P_NAME,
          '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
          1,
          CASE
            WHEN (
              LENGTH(P_NAME) - LENGTH(REPLACE(P_NAME, ' '))
            ) + 1 >= (
              LENGTH(P_NAME) - LENGTH(REPLACE(P_NAME, ' '))
            ) + 1
            AND (
              LENGTH(P_NAME) - LENGTH(REPLACE(P_NAME, ' '))
            ) >= 0
            THEN (
              LENGTH(P_NAME) - LENGTH(REPLACE(P_NAME, ' '))
            ) + 1
            ELSE NULL
          END,
          NULL,
          1
        ),
        CHR(  0)
      )
    )
  ) AS b,
  RTRIM(LTRIM(SUBSTR(P_NAME, 1, 2), 'o'), 'o') AS c,
  LPAD(TO_CHAR(P_SIZE), 3, '0') AS d,
  RPAD(TO_CHAR(P_SIZE), 3, '0') AS e,
  REPLACE(P_MFGR, 'Manufacturer#', 'm') AS f,
  REPLACE(LOWER(P_CONTAINER), ' ', '') AS g,
  CASE
    WHEN LENGTH(P_NAME) = 0 OR LENGTH(P_NAME) IS NULL
    THEN 0
    ELSE CAST((
      LENGTH(P_NAME) - NVL(LENGTH(REPLACE(P_NAME, 'o', '')), 0)
    ) AS INT)
  END + (
    (
      INSTR(P_NAME, 'o') - 1
    ) / 100.0
  ) AS h,
  ROUND(POWER(GREATEST(P_SIZE, 10), 0.5), 3) AS i
FROM TPCH.PART
ORDER BY
  1 NULLS FIRST
FETCH FIRST 5 ROWS ONLY
