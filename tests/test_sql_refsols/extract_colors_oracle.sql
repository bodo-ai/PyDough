SELECT
  P_PARTKEY AS "key",
  UPPER(
    REGEXP_SUBSTR(
      P_NAME,
      '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
      1,
      CASE
        WHEN (
          LENGTH(P_NAME) - LENGTH(REPLACE(P_NAME, ' '))
        ) >= 0
        THEN 1
        ELSE NULL
      END,
      NULL,
      1
    )
  ) AS c1,
  UPPER(
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
    )
  ) AS c2,
  UPPER(
    REGEXP_SUBSTR(
      P_NAME,
      '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
      1,
      CASE
        WHEN (
          LENGTH(P_NAME) - LENGTH(REPLACE(P_NAME, ' '))
        ) >= 2
        THEN 3
        ELSE NULL
      END,
      NULL,
      1
    )
  ) AS c3,
  UPPER(
    REGEXP_SUBSTR(
      P_NAME,
      '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
      1,
      CASE
        WHEN (
          LENGTH(P_NAME) - LENGTH(REPLACE(P_NAME, ' '))
        ) >= 3
        THEN 4
        ELSE NULL
      END,
      NULL,
      1
    )
  ) AS c4,
  UPPER(
    REGEXP_SUBSTR(
      P_NAME,
      '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
      1,
      CASE
        WHEN (
          LENGTH(P_NAME) - LENGTH(REPLACE(P_NAME, ' '))
        ) >= 4
        THEN 5
        ELSE NULL
      END,
      NULL,
      1
    )
  ) AS c5,
  UPPER(
    REGEXP_SUBSTR(
      P_NAME,
      '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
      1,
      CASE
        WHEN (
          LENGTH(P_NAME) - LENGTH(REPLACE(P_NAME, ' '))
        ) >= 5
        THEN 6
        ELSE NULL
      END,
      NULL,
      1
    )
  ) AS c6
FROM TPCH.PART
ORDER BY
  1 NULLS FIRST
FETCH FIRST 5 ROWS ONLY
