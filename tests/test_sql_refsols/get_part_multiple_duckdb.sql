SELECT
  CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) AS _expr0,
  SPLIT_PART(
    sbcustname,
    ' ',
    CASE
      WHEN CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) = 0
      THEN 1
      ELSE CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT)
    END
  ) AS p1,
  SPLIT_PART(
    sbcustname,
    ' ',
    CASE
      WHEN -1 * CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) = 0
      THEN 1
      ELSE -1 * CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT)
    END
  ) AS p2,
  SPLIT_PART(
    sbcustemail,
    '.',
    CASE
      WHEN CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) = 0
      THEN 1
      ELSE CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT)
    END
  ) AS p3,
  SPLIT_PART(
    sbcustemail,
    '.',
    CASE
      WHEN -1 * CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) = 0
      THEN 1
      ELSE -1 * CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT)
    END
  ) AS p4,
  SPLIT_PART(
    sbcustphone,
    '-',
    CASE
      WHEN CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) = 0
      THEN 1
      ELSE CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT)
    END
  ) AS p5,
  SPLIT_PART(
    sbcustphone,
    '-',
    CASE
      WHEN -1 * CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) = 0
      THEN 1
      ELSE -1 * CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT)
    END
  ) AS p6,
  SPLIT_PART(
    sbcustpostalcode,
    '00',
    CASE
      WHEN CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) = 0
      THEN 1
      ELSE CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT)
    END
  ) AS p7,
  SPLIT_PART(
    sbcustpostalcode,
    '00',
    CASE
      WHEN -1 * CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) = 0
      THEN 1
      ELSE -1 * CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT)
    END
  ) AS p8,
  SPLIT_PART(
    sbcustname,
    '!',
    CASE
      WHEN CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) = 0
      THEN 1
      ELSE CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT)
    END
  ) AS p9,
  SPLIT_PART(
    sbcustname,
    '@',
    CASE
      WHEN -1 * CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) = 0
      THEN 1
      ELSE -1 * CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT)
    END
  ) AS p10,
  SPLIT_PART(
    sbcustname,
    'aa',
    CASE
      WHEN CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) = 0
      THEN 1
      ELSE CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT)
    END
  ) AS p11,
  SPLIT_PART(
    sbcustname,
    '#$*',
    CASE
      WHEN -1 * CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) = 0
      THEN 1
      ELSE -1 * CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT)
    END
  ) AS p12,
  CASE
    WHEN ABS(
      CASE
        WHEN CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) = 0
        THEN 1
        ELSE CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT)
      END
    ) = 1
    THEN sbcustname
    ELSE NULL
  END AS p13,
  SPLIT_PART(
    '',
    ' ',
    CASE
      WHEN CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) = 0
      THEN 1
      ELSE CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT)
    END
  ) AS p14,
  SPLIT_PART(sbcustname, ' ', 1) AS p15,
  SPLIT_PART(
    sbcuststate,
    sbcuststate,
    CASE
      WHEN CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) = 0
      THEN 1
      ELSE CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT)
    END
  ) AS p16,
  SPLIT_PART(
    SPLIT_PART(sbcustphone, '-', 1),
    '5',
    CASE
      WHEN CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) = 0
      THEN 1
      ELSE CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT)
    END
  ) AS p17,
  SPLIT_PART(
    sbcustpostalcode,
    '0',
    CASE
      WHEN CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) = 0
      THEN 1
      ELSE CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT)
    END
  ) AS p18
FROM main.sbcustomer
WHERE
  CAST(TRUNC(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE)) AS BIGINT) <= 4
ORDER BY
  1 NULLS FIRST
