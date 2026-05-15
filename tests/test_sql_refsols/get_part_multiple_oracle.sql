SELECT
  TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') AS "_expr0",
  REGEXP_SUBSTR(
    sbcustname,
    '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' '))
        ) / LENGTH(' ')
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' '))
          ) / LENGTH(' ')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' '))
          ) / LENGTH(' ')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' '))
          ) / LENGTH(' ')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p1,
  REGEXP_SUBSTR(
    sbcustname,
    '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' '))
        ) / LENGTH(' ')
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' '))
          ) / LENGTH(' ')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' '))
          ) / LENGTH(' ')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' '))
          ) / LENGTH(' ')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p2,
  REGEXP_SUBSTR(
    sbcustemail,
    '(.*?)(' || REGEXP_REPLACE('.', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.'))
        ) / LENGTH('.')
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.'))
          ) / LENGTH('.')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.'))
          ) / LENGTH('.')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.'))
          ) / LENGTH('.')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p3,
  REGEXP_SUBSTR(
    sbcustemail,
    '(.*?)(' || REGEXP_REPLACE('.', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.'))
        ) / LENGTH('.')
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.'))
          ) / LENGTH('.')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.'))
          ) / LENGTH('.')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.'))
          ) / LENGTH('.')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p4,
  REGEXP_SUBSTR(
    sbcustphone,
    '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-'))
        ) / LENGTH('-')
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-'))
          ) / LENGTH('-')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-'))
          ) / LENGTH('-')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-'))
          ) / LENGTH('-')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p5,
  REGEXP_SUBSTR(
    sbcustphone,
    '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-'))
        ) / LENGTH('-')
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-'))
          ) / LENGTH('-')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-'))
          ) / LENGTH('-')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-'))
          ) / LENGTH('-')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p6,
  REGEXP_SUBSTR(
    sbcustpostalcode,
    '(.*?)(' || REGEXP_REPLACE('00', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00'))
        ) / LENGTH('00')
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00'))
          ) / LENGTH('00')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00'))
          ) / LENGTH('00')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00'))
          ) / LENGTH('00')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p7,
  REGEXP_SUBSTR(
    sbcustpostalcode,
    '(.*?)(' || REGEXP_REPLACE('00', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00'))
        ) / LENGTH('00')
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00'))
          ) / LENGTH('00')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00'))
          ) / LENGTH('00')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00'))
          ) / LENGTH('00')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p8,
  REGEXP_SUBSTR(
    sbcustname,
    '(.*?)(' || REGEXP_REPLACE('!', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '!'))
        ) / LENGTH('!')
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '!'))
          ) / LENGTH('!')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '!'))
          ) / LENGTH('!')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '!'))
          ) / LENGTH('!')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p9,
  REGEXP_SUBSTR(
    sbcustname,
    '(.*?)(' || REGEXP_REPLACE('@', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '@'))
        ) / LENGTH('@')
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '@'))
          ) / LENGTH('@')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '@'))
          ) / LENGTH('@')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '@'))
          ) / LENGTH('@')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p10,
  REGEXP_SUBSTR(
    sbcustname,
    '(.*?)(' || REGEXP_REPLACE('aa', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, 'aa'))
        ) / LENGTH('aa')
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, 'aa'))
          ) / LENGTH('aa')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, 'aa'))
          ) / LENGTH('aa')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, 'aa'))
          ) / LENGTH('aa')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p11,
  REGEXP_SUBSTR(
    sbcustname,
    '(.*?)(' || REGEXP_REPLACE('#$*', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '#$*'))
        ) / LENGTH('#$*')
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '#$*'))
          ) / LENGTH('#$*')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '#$*'))
          ) / LENGTH('#$*')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ) > 0
        THEN 0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '#$*'))
          ) / LENGTH('#$*')
        ) + 2 + (
          0 - TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        )
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p12,
  CASE
    WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 1
    THEN sbcustname
    ELSE NULL
  END AS p13,
  REGEXP_SUBSTR(
    '',
    '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH('') - LENGTH(REPLACE('', ' '))
        ) / LENGTH(' ')
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH('') - LENGTH(REPLACE('', ' '))
          ) / LENGTH(' ')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH('') - LENGTH(REPLACE('', ' '))
          ) / LENGTH(' ')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH('') - LENGTH(REPLACE('', ' '))
          ) / LENGTH(' ')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p14,
  REGEXP_SUBSTR(
    sbcustname,
    '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' '))
        ) / LENGTH(' ')
      ) >= 0
      THEN 1
      ELSE NULL
    END,
    NULL,
    1
  ) AS p15,
  CASE
    WHEN sbcuststate = '' OR sbcuststate IS NULL
    THEN CASE
      WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 1
      THEN sbcuststate
      ELSE NULL
    END
    ELSE REGEXP_SUBSTR(
      sbcuststate,
      '(.*?)(' || REGEXP_REPLACE(sbcuststate, '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
      1,
      CASE
        WHEN (
          (
            LENGTH(sbcuststate) - LENGTH(REPLACE(sbcuststate, sbcuststate))
          ) / LENGTH(sbcuststate)
        ) + 1 >= CASE
          WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
          THEN 1
          WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
          THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
          ELSE (
            (
              LENGTH(sbcuststate) - LENGTH(REPLACE(sbcuststate, sbcuststate))
            ) / LENGTH(sbcuststate)
          ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        END
        AND CASE
          WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
          THEN 1
          WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
          THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
          ELSE (
            (
              LENGTH(sbcuststate) - LENGTH(REPLACE(sbcuststate, sbcuststate))
            ) / LENGTH(sbcuststate)
          ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        END >= 1
        THEN CASE
          WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
          THEN 1
          WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
          THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
          ELSE (
            (
              LENGTH(sbcuststate) - LENGTH(REPLACE(sbcuststate, sbcuststate))
            ) / LENGTH(sbcuststate)
          ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        END
        ELSE NULL
      END,
      NULL,
      1
    )
  END AS p16,
  REGEXP_SUBSTR(
    REGEXP_SUBSTR(
      sbcustphone,
      '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
      1,
      CASE
        WHEN (
          (
            LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-'))
          ) / LENGTH('-')
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
        (
          LENGTH(
            REGEXP_SUBSTR(
              sbcustphone,
              '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
              1,
              CASE
                WHEN (
                  (
                    LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-'))
                  ) / LENGTH('-')
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
                sbcustphone,
                '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
                1,
                CASE
                  WHEN (
                    (
                      LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-'))
                    ) / LENGTH('-')
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
        ) / LENGTH('5')
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(
              REGEXP_SUBSTR(
                sbcustphone,
                '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
                1,
                CASE
                  WHEN (
                    (
                      LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-'))
                    ) / LENGTH('-')
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
                  sbcustphone,
                  '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
                  1,
                  CASE
                    WHEN (
                      (
                        LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-'))
                      ) / LENGTH('-')
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
          ) / LENGTH('5')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(
              REGEXP_SUBSTR(
                sbcustphone,
                '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
                1,
                CASE
                  WHEN (
                    (
                      LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-'))
                    ) / LENGTH('-')
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
                  sbcustphone,
                  '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
                  1,
                  CASE
                    WHEN (
                      (
                        LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-'))
                      ) / LENGTH('-')
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
          ) / LENGTH('5')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(
              REGEXP_SUBSTR(
                sbcustphone,
                '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
                1,
                CASE
                  WHEN (
                    (
                      LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-'))
                    ) / LENGTH('-')
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
                  sbcustphone,
                  '(.*?)(' || REGEXP_REPLACE('-', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
                  1,
                  CASE
                    WHEN (
                      (
                        LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-'))
                      ) / LENGTH('-')
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
          ) / LENGTH('5')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p17,
  REGEXP_SUBSTR(
    sbcustpostalcode,
    '(.*?)(' || REGEXP_REPLACE('0', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        (
          LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '0'))
        ) / LENGTH('0')
      ) + 1 >= CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '0'))
          ) / LENGTH('0')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      AND CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '0'))
          ) / LENGTH('0')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END >= 1
      THEN CASE
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') = 0
        THEN 1
        WHEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') > 0
        THEN TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
        ELSE (
          (
            LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '0'))
          ) / LENGTH('0')
        ) + 2 + TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0')
      END
      ELSE NULL
    END,
    NULL,
    1
  ) AS p18
FROM MAIN.SBCUSTOMER
WHERE
  TRUNC(CAST(SUBSTR(sbcustid, 2) AS DOUBLE PRECISION), '0') <= 4
ORDER BY
  1 NULLS FIRST
