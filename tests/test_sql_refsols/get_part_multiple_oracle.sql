SELECT
  CAST(SUBSTR(sbcustid, 2) AS INT) AS _expr0,
  REGEXP_SUBSTR(
    sbcustname,
    '[^ ]+',
    1,
    CASE
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) = 0
      THEN 1
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) > 0
      THEN CAST(SUBSTR(sbcustid, 2) AS INT)
      ELSE REGEXP_COUNT(sbcustname, ' ') + 2 + CAST(SUBSTR(sbcustid, 2) AS INT)
    END
  ) AS p1,
  REGEXP_SUBSTR(
    sbcustname,
    '[^ ]+',
    1,
    CASE
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) = 0
      THEN 1
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) > 0
      THEN 0 - CAST(SUBSTR(sbcustid, 2) AS INT)
      ELSE REGEXP_COUNT(sbcustname, ' ') + 1 - CAST(SUBSTR(sbcustid, 2) AS INT) + 1
    END
  ) AS p2,
  REGEXP_SUBSTR(
    sbcustemail,
    '[^.]+',
    1,
    CASE
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) = 0
      THEN 1
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) > 0
      THEN CAST(SUBSTR(sbcustid, 2) AS INT)
      ELSE REGEXP_COUNT(sbcustemail, '.') + 2 + CAST(SUBSTR(sbcustid, 2) AS INT)
    END
  ) AS p3,
  REGEXP_SUBSTR(
    sbcustemail,
    '[^.]+',
    1,
    CASE
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) = 0
      THEN 1
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) > 0
      THEN 0 - CAST(SUBSTR(sbcustid, 2) AS INT)
      ELSE REGEXP_COUNT(sbcustemail, '.') + 1 - CAST(SUBSTR(sbcustid, 2) AS INT) + 1
    END
  ) AS p4,
  REGEXP_SUBSTR(
    sbcustphone,
    '[^-]+',
    1,
    CASE
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) = 0
      THEN 1
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) > 0
      THEN CAST(SUBSTR(sbcustid, 2) AS INT)
      ELSE REGEXP_COUNT(sbcustphone, '-') + 2 + CAST(SUBSTR(sbcustid, 2) AS INT)
    END
  ) AS p5,
  REGEXP_SUBSTR(
    sbcustphone,
    '[^-]+',
    1,
    CASE
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) = 0
      THEN 1
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) > 0
      THEN 0 - CAST(SUBSTR(sbcustid, 2) AS INT)
      ELSE REGEXP_COUNT(sbcustphone, '-') + 1 - CAST(SUBSTR(sbcustid, 2) AS INT) + 1
    END
  ) AS p6,
  REGEXP_SUBSTR(
    sbcustpostalcode,
    '[^00]+',
    1,
    CASE
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) = 0
      THEN 1
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) > 0
      THEN CAST(SUBSTR(sbcustid, 2) AS INT)
      ELSE REGEXP_COUNT(sbcustpostalcode, '00') + 2 + CAST(SUBSTR(sbcustid, 2) AS INT)
    END
  ) AS p7,
  REGEXP_SUBSTR(
    sbcustpostalcode,
    '[^00]+',
    1,
    CASE
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) = 0
      THEN 1
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) > 0
      THEN 0 - CAST(SUBSTR(sbcustid, 2) AS INT)
      ELSE REGEXP_COUNT(sbcustpostalcode, '00') + 1 - CAST(SUBSTR(sbcustid, 2) AS INT) + 1
    END
  ) AS p8,
  REGEXP_SUBSTR(
    sbcustname,
    '[^!]+',
    1,
    CASE
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) = 0
      THEN 1
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) > 0
      THEN CAST(SUBSTR(sbcustid, 2) AS INT)
      ELSE REGEXP_COUNT(sbcustname, '!') + 2 + CAST(SUBSTR(sbcustid, 2) AS INT)
    END
  ) AS p9,
  REGEXP_SUBSTR(
    sbcustname,
    '[^@]+',
    1,
    CASE
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) = 0
      THEN 1
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) > 0
      THEN 0 - CAST(SUBSTR(sbcustid, 2) AS INT)
      ELSE REGEXP_COUNT(sbcustname, '@') + 1 - CAST(SUBSTR(sbcustid, 2) AS INT) + 1
    END
  ) AS p10,
  REGEXP_SUBSTR(
    sbcustname,
    '[^aa]+',
    1,
    CASE
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) = 0
      THEN 1
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) > 0
      THEN CAST(SUBSTR(sbcustid, 2) AS INT)
      ELSE REGEXP_COUNT(sbcustname, 'aa') + 2 + CAST(SUBSTR(sbcustid, 2) AS INT)
    END
  ) AS p11,
  REGEXP_SUBSTR(
    sbcustname,
    '[^#$*]+',
    1,
    CASE
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) = 0
      THEN 1
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) > 0
      THEN 0 - CAST(SUBSTR(sbcustid, 2) AS INT)
      ELSE REGEXP_COUNT(sbcustname, '#$*') + 1 - CAST(SUBSTR(sbcustid, 2) AS INT) + 1
    END
  ) AS p12,
  REGEXP_SUBSTR(
    sbcustname,
    '[^]+',
    1,
    CASE
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) = 0
      THEN 1
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) > 0
      THEN CAST(SUBSTR(sbcustid, 2) AS INT)
      ELSE REGEXP_COUNT(sbcustname, '') + 2 + CAST(SUBSTR(sbcustid, 2) AS INT)
    END
  ) AS p13,
  REGEXP_SUBSTR(
    '',
    '[^ ]+',
    1,
    CASE
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) = 0
      THEN 1
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) > 0
      THEN CAST(SUBSTR(sbcustid, 2) AS INT)
      ELSE REGEXP_COUNT('', ' ') + 2 + CAST(SUBSTR(sbcustid, 2) AS INT)
    END
  ) AS p14,
  REGEXP_SUBSTR(sbcustname, '[^ ]+', 1, 1) AS p15,
  REGEXP_SUBSTR(
    sbcuststate,
    '[^' || sbcuststate || ']+',
    1,
    CASE
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) = 0
      THEN 1
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) > 0
      THEN CAST(SUBSTR(sbcustid, 2) AS INT)
      ELSE REGEXP_COUNT(sbcuststate, sbcuststate) + 2 + CAST(SUBSTR(sbcustid, 2) AS INT)
    END
  ) AS p16,
  REGEXP_SUBSTR(
    REGEXP_SUBSTR(sbcustphone, '[^-]+', 1, 1),
    '[^5]+',
    1,
    CASE
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) = 0
      THEN 1
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) > 0
      THEN CAST(SUBSTR(sbcustid, 2) AS INT)
      ELSE REGEXP_COUNT(REGEXP_SUBSTR(sbcustphone, '[^-]+', 1, 1), '5') + 2 + CAST(SUBSTR(sbcustid, 2) AS INT)
    END
  ) AS p17,
  REGEXP_SUBSTR(
    sbcustpostalcode,
    '[^0]+',
    1,
    CASE
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) = 0
      THEN 1
      WHEN CAST(SUBSTR(sbcustid, 2) AS INT) > 0
      THEN CAST(SUBSTR(sbcustid, 2) AS INT)
      ELSE REGEXP_COUNT(sbcustpostalcode, '0') + 2 + CAST(SUBSTR(sbcustid, 2) AS INT)
    END
  ) AS p18
FROM MAIN.SBCUSTOMER
WHERE
  CAST(SUBSTR(sbcustid, 2) AS INT) <= 4
ORDER BY
  1 NULLS FIRST
