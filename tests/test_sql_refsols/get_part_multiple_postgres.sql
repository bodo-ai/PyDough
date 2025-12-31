SELECT
  TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) AS _expr0,
  SPLIT_PART(
    sbcustname,
    ' ',
    CASE
      WHEN TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) = 0
      THEN 1
      ELSE TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL))
    END
  ) AS p1,
  SPLIT_PART(
    sbcustname,
    ' ',
    CASE
      WHEN TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) = 0
      THEN 1
      ELSE 0 - TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL))
    END
  ) AS p2,
  SPLIT_PART(
    sbcustemail,
    '.',
    CASE
      WHEN TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) = 0
      THEN 1
      ELSE TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL))
    END
  ) AS p3,
  SPLIT_PART(
    sbcustemail,
    '.',
    CASE
      WHEN TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) = 0
      THEN 1
      ELSE 0 - TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL))
    END
  ) AS p4,
  SPLIT_PART(
    sbcustphone,
    '-',
    CASE
      WHEN TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) = 0
      THEN 1
      ELSE TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL))
    END
  ) AS p5,
  SPLIT_PART(
    sbcustphone,
    '-',
    CASE
      WHEN TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) = 0
      THEN 1
      ELSE 0 - TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL))
    END
  ) AS p6,
  SPLIT_PART(
    sbcustpostalcode,
    '00',
    CASE
      WHEN TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) = 0
      THEN 1
      ELSE TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL))
    END
  ) AS p7,
  SPLIT_PART(
    sbcustpostalcode,
    '00',
    CASE
      WHEN TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) = 0
      THEN 1
      ELSE 0 - TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL))
    END
  ) AS p8,
  SPLIT_PART(
    sbcustname,
    '!',
    CASE
      WHEN TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) = 0
      THEN 1
      ELSE TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL))
    END
  ) AS p9,
  SPLIT_PART(
    sbcustname,
    '@',
    CASE
      WHEN TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) = 0
      THEN 1
      ELSE 0 - TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL))
    END
  ) AS p10,
  SPLIT_PART(
    sbcustname,
    'aa',
    CASE
      WHEN TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) = 0
      THEN 1
      ELSE TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL))
    END
  ) AS p11,
  SPLIT_PART(
    sbcustname,
    '#$*',
    CASE
      WHEN TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) = 0
      THEN 1
      ELSE 0 - TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL))
    END
  ) AS p12,
  SPLIT_PART(
    sbcustname,
    '',
    CASE
      WHEN TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) = 0
      THEN 1
      ELSE TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL))
    END
  ) AS p13,
  SPLIT_PART(
    '',
    ' ',
    CASE
      WHEN TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) = 0
      THEN 1
      ELSE TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL))
    END
  ) AS p14,
  SPLIT_PART(sbcustname, ' ', 1) AS p15,
  SPLIT_PART(
    sbcuststate,
    sbcuststate,
    CASE
      WHEN TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) = 0
      THEN 1
      ELSE TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL))
    END
  ) AS p16,
  SPLIT_PART(
    SPLIT_PART(sbcustphone, '-', 1),
    '5',
    CASE
      WHEN TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) = 0
      THEN 1
      ELSE TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL))
    END
  ) AS p17,
  SPLIT_PART(
    sbcustpostalcode,
    '0',
    CASE
      WHEN TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) = 0
      THEN 1
      ELSE TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL))
    END
  ) AS p18
FROM main.sbcustomer
WHERE
  TRUNC(CAST(SUBSTRING(sbcustid FROM 2) AS DECIMAL)) <= 4
ORDER BY
  1 NULLS FIRST
