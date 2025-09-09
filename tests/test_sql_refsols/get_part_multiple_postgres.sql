SELECT
  CAST(SUBSTRING(sbcustid FROM 2) AS INT) AS _expr0,
  SPLIT_PART(
    sbcustname,
    ' ',
    CASE
      WHEN CAST(SUBSTRING(sbcustid FROM 2) AS INT) = 0
      THEN 1
      ELSE CAST(SUBSTRING(sbcustid FROM 2) AS INT)
    END
  ) AS p1,
  SPLIT_PART(
    sbcustname,
    ' ',
    CASE
      WHEN CAST(SUBSTRING(sbcustid FROM 2) AS INT) = 0
      THEN 1
      ELSE 0 - CAST(SUBSTRING(sbcustid FROM 2) AS INT)
    END
  ) AS p2,
  SPLIT_PART(
    sbcustemail,
    '.',
    CASE
      WHEN CAST(SUBSTRING(sbcustid FROM 2) AS INT) = 0
      THEN 1
      ELSE CAST(SUBSTRING(sbcustid FROM 2) AS INT)
    END
  ) AS p3,
  SPLIT_PART(
    sbcustemail,
    '.',
    CASE
      WHEN CAST(SUBSTRING(sbcustid FROM 2) AS INT) = 0
      THEN 1
      ELSE 0 - CAST(SUBSTRING(sbcustid FROM 2) AS INT)
    END
  ) AS p4,
  SPLIT_PART(
    sbcustphone,
    '-',
    CASE
      WHEN CAST(SUBSTRING(sbcustid FROM 2) AS INT) = 0
      THEN 1
      ELSE CAST(SUBSTRING(sbcustid FROM 2) AS INT)
    END
  ) AS p5,
  SPLIT_PART(
    sbcustphone,
    '-',
    CASE
      WHEN CAST(SUBSTRING(sbcustid FROM 2) AS INT) = 0
      THEN 1
      ELSE 0 - CAST(SUBSTRING(sbcustid FROM 2) AS INT)
    END
  ) AS p6,
  SPLIT_PART(
    sbcustpostalcode,
    '00',
    CASE
      WHEN CAST(SUBSTRING(sbcustid FROM 2) AS INT) = 0
      THEN 1
      ELSE CAST(SUBSTRING(sbcustid FROM 2) AS INT)
    END
  ) AS p7,
  SPLIT_PART(
    sbcustpostalcode,
    '00',
    CASE
      WHEN CAST(SUBSTRING(sbcustid FROM 2) AS INT) = 0
      THEN 1
      ELSE 0 - CAST(SUBSTRING(sbcustid FROM 2) AS INT)
    END
  ) AS p8,
  SPLIT_PART(
    sbcustname,
    '!',
    CASE
      WHEN CAST(SUBSTRING(sbcustid FROM 2) AS INT) = 0
      THEN 1
      ELSE CAST(SUBSTRING(sbcustid FROM 2) AS INT)
    END
  ) AS p9,
  SPLIT_PART(
    sbcustname,
    '@',
    CASE
      WHEN CAST(SUBSTRING(sbcustid FROM 2) AS INT) = 0
      THEN 1
      ELSE 0 - CAST(SUBSTRING(sbcustid FROM 2) AS INT)
    END
  ) AS p10,
  SPLIT_PART(
    sbcustname,
    'aa',
    CASE
      WHEN CAST(SUBSTRING(sbcustid FROM 2) AS INT) = 0
      THEN 1
      ELSE CAST(SUBSTRING(sbcustid FROM 2) AS INT)
    END
  ) AS p11,
  SPLIT_PART(
    sbcustname,
    '#$*',
    CASE
      WHEN CAST(SUBSTRING(sbcustid FROM 2) AS INT) = 0
      THEN 1
      ELSE 0 - CAST(SUBSTRING(sbcustid FROM 2) AS INT)
    END
  ) AS p12,
  SPLIT_PART(
    sbcustname,
    '',
    CASE
      WHEN CAST(SUBSTRING(sbcustid FROM 2) AS INT) = 0
      THEN 1
      ELSE CAST(SUBSTRING(sbcustid FROM 2) AS INT)
    END
  ) AS p13,
  SPLIT_PART(
    '',
    ' ',
    CASE
      WHEN CAST(SUBSTRING(sbcustid FROM 2) AS INT) = 0
      THEN 1
      ELSE CAST(SUBSTRING(sbcustid FROM 2) AS INT)
    END
  ) AS p14,
  SPLIT_PART(sbcustname, ' ', 1) AS p15,
  SPLIT_PART(
    sbcuststate,
    sbcuststate,
    CASE
      WHEN CAST(SUBSTRING(sbcustid FROM 2) AS INT) = 0
      THEN 1
      ELSE CAST(SUBSTRING(sbcustid FROM 2) AS INT)
    END
  ) AS p16,
  SPLIT_PART(
    SPLIT_PART(sbcustphone, '-', 1),
    '5',
    CASE
      WHEN CAST(SUBSTRING(sbcustid FROM 2) AS INT) = 0
      THEN 1
      ELSE CAST(SUBSTRING(sbcustid FROM 2) AS INT)
    END
  ) AS p17,
  SPLIT_PART(
    sbcustpostalcode,
    '0',
    CASE
      WHEN CAST(SUBSTRING(sbcustid FROM 2) AS INT) = 0
      THEN 1
      ELSE CAST(SUBSTRING(sbcustid FROM 2) AS INT)
    END
  ) AS p18
FROM main.sbcustomer
WHERE
  CAST(SUBSTRING(sbcustid FROM 2) AS INT) <= 4
ORDER BY
  1 NULLS FIRST
