SELECT
  TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) AS "_expr0",
  REGEXP_SUBSTR(
    SBCUSTNAME,
    '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, ' '))
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, ' '))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, ' '))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, ' '))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p1,
  REGEXP_SUBSTR(
    SBCUSTNAME,
    '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, ' '))
      ) + 1 >= CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, ' '))
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      AND CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, ' '))
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END >= 1
      THEN CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, ' '))
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p2,
  REGEXP_SUBSTR(
    SBCUSTEMAIL,
    '(.*?)(' || REGEXP_REPLACE('.', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        LENGTH(SBCUSTEMAIL) - LENGTH(REPLACE(SBCUSTEMAIL, '.'))
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTEMAIL) - LENGTH(REPLACE(SBCUSTEMAIL, '.'))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTEMAIL) - LENGTH(REPLACE(SBCUSTEMAIL, '.'))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTEMAIL) - LENGTH(REPLACE(SBCUSTEMAIL, '.'))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p3,
  REGEXP_SUBSTR(
    SBCUSTEMAIL,
    '(.*?)(' || REGEXP_REPLACE('.', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        LENGTH(SBCUSTEMAIL) - LENGTH(REPLACE(SBCUSTEMAIL, '.'))
      ) + 1 >= CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTEMAIL) - LENGTH(REPLACE(SBCUSTEMAIL, '.'))
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      AND CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTEMAIL) - LENGTH(REPLACE(SBCUSTEMAIL, '.'))
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END >= 1
      THEN CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTEMAIL) - LENGTH(REPLACE(SBCUSTEMAIL, '.'))
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p4,
  REGEXP_SUBSTR(
    SBCUSTPHONE,
    '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        LENGTH(SBCUSTPHONE) - LENGTH(REPLACE(SBCUSTPHONE, '-'))
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTPHONE) - LENGTH(REPLACE(SBCUSTPHONE, '-'))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTPHONE) - LENGTH(REPLACE(SBCUSTPHONE, '-'))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTPHONE) - LENGTH(REPLACE(SBCUSTPHONE, '-'))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p5,
  REGEXP_SUBSTR(
    SBCUSTPHONE,
    '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        LENGTH(SBCUSTPHONE) - LENGTH(REPLACE(SBCUSTPHONE, '-'))
      ) + 1 >= CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTPHONE) - LENGTH(REPLACE(SBCUSTPHONE, '-'))
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      AND CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTPHONE) - LENGTH(REPLACE(SBCUSTPHONE, '-'))
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END >= 1
      THEN CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTPHONE) - LENGTH(REPLACE(SBCUSTPHONE, '-'))
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p6,
  REGEXP_SUBSTR(
    SBCUSTPOSTALCODE,
    '(.*?)(' || REGEXP_REPLACE('00', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(SBCUSTPOSTALCODE) - LENGTH(REPLACE(SBCUSTPOSTALCODE, '00'))
        ) / 2
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          (
            LENGTH(SBCUSTPOSTALCODE) - LENGTH(REPLACE(SBCUSTPOSTALCODE, '00'))
          ) / 2
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          (
            LENGTH(SBCUSTPOSTALCODE) - LENGTH(REPLACE(SBCUSTPOSTALCODE, '00'))
          ) / 2
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          (
            LENGTH(SBCUSTPOSTALCODE) - LENGTH(REPLACE(SBCUSTPOSTALCODE, '00'))
          ) / 2
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p7,
  REGEXP_SUBSTR(
    SBCUSTPOSTALCODE,
    '(.*?)(' || REGEXP_REPLACE('00', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(SBCUSTPOSTALCODE) - LENGTH(REPLACE(SBCUSTPOSTALCODE, '00'))
        ) / 2
      ) + 1 >= CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          (
            LENGTH(SBCUSTPOSTALCODE) - LENGTH(REPLACE(SBCUSTPOSTALCODE, '00'))
          ) / 2
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      AND CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          (
            LENGTH(SBCUSTPOSTALCODE) - LENGTH(REPLACE(SBCUSTPOSTALCODE, '00'))
          ) / 2
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END >= 1
      THEN CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          (
            LENGTH(SBCUSTPOSTALCODE) - LENGTH(REPLACE(SBCUSTPOSTALCODE, '00'))
          ) / 2
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p8,
  REGEXP_SUBSTR(
    SBCUSTNAME,
    '(.*?)(' || REGEXP_REPLACE('!', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, '!'))
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, '!'))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, '!'))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, '!'))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p9,
  REGEXP_SUBSTR(
    SBCUSTNAME,
    '(.*?)(' || REGEXP_REPLACE('@', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, '@'))
      ) + 1 >= CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, '@'))
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      AND CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, '@'))
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END >= 1
      THEN CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, '@'))
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p10,
  REGEXP_SUBSTR(
    SBCUSTNAME,
    '(.*?)(' || REGEXP_REPLACE('aa', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, 'aa'))
        ) / 2
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          (
            LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, 'aa'))
          ) / 2
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          (
            LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, 'aa'))
          ) / 2
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          (
            LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, 'aa'))
          ) / 2
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p11,
  REGEXP_SUBSTR(
    SBCUSTNAME,
    '(.*?)(' || REGEXP_REPLACE('#$*', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, '#$*'))
        ) / 3
      ) + 1 >= CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          (
            LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, '#$*'))
          ) / 3
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      AND CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          (
            LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, '#$*'))
          ) / 3
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END >= 1
      THEN CASE
        WHEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN (
          -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ) > 0
        THEN -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          (
            LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, '#$*'))
          ) / 3
        ) + 2 + -1 * TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p12,
  CASE
    WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 1
    THEN SBCUSTNAME
    ELSE NULL
  END AS p13,
  REGEXP_SUBSTR(
    '',
    '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        0 - LENGTH(REPLACE('', ' '))
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          0 - LENGTH(REPLACE('', ' '))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          0 - LENGTH(REPLACE('', ' '))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          0 - LENGTH(REPLACE('', ' '))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p14,
  REGEXP_SUBSTR(
    SBCUSTNAME,
    '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, ' '))
      ) >= 0
      THEN 1
      ELSE NULL
    END,
    NULL,
    1
  ) AS p15,
  CASE
    WHEN SBCUSTSTATE = '' OR SBCUSTSTATE IS NULL
    THEN CASE
      WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 1
      THEN SBCUSTSTATE
      ELSE NULL
    END
    ELSE REGEXP_SUBSTR(
      SBCUSTSTATE,
      '(.*?)(' || REGEXP_REPLACE(SBCUSTSTATE, '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
      1,
      CASE
        WHEN (
          (
            LENGTH(SBCUSTSTATE) - LENGTH(REPLACE(SBCUSTSTATE, SBCUSTSTATE))
          ) / LENGTH(SBCUSTSTATE)
        ) + 1 >= CASE
          WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
          THEN 1
          WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
          THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
          ELSE (
            (
              LENGTH(SBCUSTSTATE) - LENGTH(REPLACE(SBCUSTSTATE, SBCUSTSTATE))
            ) / LENGTH(SBCUSTSTATE)
          ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        END
        AND CASE
          WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
          THEN 1
          WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
          THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
          ELSE (
            (
              LENGTH(SBCUSTSTATE) - LENGTH(REPLACE(SBCUSTSTATE, SBCUSTSTATE))
            ) / LENGTH(SBCUSTSTATE)
          ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        END >= 1
        THEN CASE
          WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
          THEN 1
          WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
          THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
          ELSE (
            (
              LENGTH(SBCUSTSTATE) - LENGTH(REPLACE(SBCUSTSTATE, SBCUSTSTATE))
            ) / LENGTH(SBCUSTSTATE)
          ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        END
        ELSE NULL
      END,
      NULL,
      1
    )
  END AS p16,
  REGEXP_SUBSTR(
    REGEXP_SUBSTR(
      SBCUSTPHONE,
      '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
      1,
      CASE
        WHEN (
          LENGTH(SBCUSTPHONE) - LENGTH(REPLACE(SBCUSTPHONE, '-'))
        ) >= 0
        THEN 1
        ELSE NULL
      END,
      NULL,
      1
    ),
    '(.*?)(' || REGEXP_REPLACE('5', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        LENGTH(
          REGEXP_SUBSTR(
            SBCUSTPHONE,
            '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
            1,
            CASE
              WHEN (
                LENGTH(SBCUSTPHONE) - LENGTH(REPLACE(SBCUSTPHONE, '-'))
              ) >= 0
              THEN 1
              ELSE NULL
            END,
            NULL,
            1
          )
        ) - LENGTH(
          REPLACE(
            REGEXP_SUBSTR(
              SBCUSTPHONE,
              '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
              1,
              CASE
                WHEN (
                  LENGTH(SBCUSTPHONE) - LENGTH(REPLACE(SBCUSTPHONE, '-'))
                ) >= 0
                THEN 1
                ELSE NULL
              END,
              NULL,
              1
            ),
            '5'
          )
        )
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(
            REGEXP_SUBSTR(
              SBCUSTPHONE,
              '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
              1,
              CASE
                WHEN (
                  LENGTH(SBCUSTPHONE) - LENGTH(REPLACE(SBCUSTPHONE, '-'))
                ) >= 0
                THEN 1
                ELSE NULL
              END,
              NULL,
              1
            )
          ) - LENGTH(
            REPLACE(
              REGEXP_SUBSTR(
                SBCUSTPHONE,
                '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
                1,
                CASE
                  WHEN (
                    LENGTH(SBCUSTPHONE) - LENGTH(REPLACE(SBCUSTPHONE, '-'))
                  ) >= 0
                  THEN 1
                  ELSE NULL
                END,
                NULL,
                1
              ),
              '5'
            )
          )
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(
            REGEXP_SUBSTR(
              SBCUSTPHONE,
              '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
              1,
              CASE
                WHEN (
                  LENGTH(SBCUSTPHONE) - LENGTH(REPLACE(SBCUSTPHONE, '-'))
                ) >= 0
                THEN 1
                ELSE NULL
              END,
              NULL,
              1
            )
          ) - LENGTH(
            REPLACE(
              REGEXP_SUBSTR(
                SBCUSTPHONE,
                '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
                1,
                CASE
                  WHEN (
                    LENGTH(SBCUSTPHONE) - LENGTH(REPLACE(SBCUSTPHONE, '-'))
                  ) >= 0
                  THEN 1
                  ELSE NULL
                END,
                NULL,
                1
              ),
              '5'
            )
          )
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(
            REGEXP_SUBSTR(
              SBCUSTPHONE,
              '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
              1,
              CASE
                WHEN (
                  LENGTH(SBCUSTPHONE) - LENGTH(REPLACE(SBCUSTPHONE, '-'))
                ) >= 0
                THEN 1
                ELSE NULL
              END,
              NULL,
              1
            )
          ) - LENGTH(
            REPLACE(
              REGEXP_SUBSTR(
                SBCUSTPHONE,
                '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
                1,
                CASE
                  WHEN (
                    LENGTH(SBCUSTPHONE) - LENGTH(REPLACE(SBCUSTPHONE, '-'))
                  ) >= 0
                  THEN 1
                  ELSE NULL
                END,
                NULL,
                1
              ),
              '5'
            )
          )
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p17,
  REGEXP_SUBSTR(
    SBCUSTPOSTALCODE,
    '(.*?)(' || REGEXP_REPLACE('0', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        LENGTH(SBCUSTPOSTALCODE) - LENGTH(REPLACE(SBCUSTPOSTALCODE, '0'))
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTPOSTALCODE) - LENGTH(REPLACE(SBCUSTPOSTALCODE, '0'))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTPOSTALCODE) - LENGTH(REPLACE(SBCUSTPOSTALCODE, '0'))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) > 0
        THEN TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
        ELSE (
          LENGTH(SBCUSTPOSTALCODE) - LENGTH(REPLACE(SBCUSTPOSTALCODE, '0'))
        ) + 2 + TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0)
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p18
FROM MAIN.SBCUSTOMER
WHERE
  TRUNC(CAST(SUBSTR(SBCUSTID, 2) AS DOUBLE PRECISION), 0) <= 4
ORDER BY
  1 NULLS FIRST
