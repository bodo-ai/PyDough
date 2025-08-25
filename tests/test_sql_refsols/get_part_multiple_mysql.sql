SELECT
  TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) AS _expr0,
  CASE
    WHEN CHAR_LENGTH(sbcustname) = 0
    THEN NULL
    WHEN CHAR_LENGTH(' ') = 0
    THEN CASE
      WHEN ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)) = 1
      THEN sbcustname
      ELSE NULL
    END
    WHEN (
      CHAR_LENGTH(sbcustname) - CHAR_LENGTH(REPLACE(sbcustname, ' ', ''))
    ) / CHAR_LENGTH(' ') + 1 >= TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustname, ' ', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      ' ',
      -1
    )
    WHEN (
      CHAR_LENGTH(sbcustname) - CHAR_LENGTH(REPLACE(sbcustname, ' ', ''))
    ) / CHAR_LENGTH(' ') + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustname, ' ', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      ' ',
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustname, ' ', 1), ' ', -1)
    ELSE NULL
  END AS p1,
  CASE
    WHEN CHAR_LENGTH(sbcustname) = 0
    THEN NULL
    WHEN CHAR_LENGTH(' ') = 0
    THEN CASE
      WHEN ABS((
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )) = 1
      THEN sbcustname
      ELSE NULL
    END
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) <= (
      CHAR_LENGTH(sbcustname) - CHAR_LENGTH(REPLACE(sbcustname, ' ', ''))
    ) / CHAR_LENGTH(' ') + 1
    AND (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustname, ' ', (
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )),
      ' ',
      -1
    )
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) < 0
    AND (
      CHAR_LENGTH(sbcustname) - CHAR_LENGTH(REPLACE(sbcustname, ' ', ''))
    ) / CHAR_LENGTH(' ') + 1 >= ABS((
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ))
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustname, ' ', (
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )),
      ' ',
      1
    )
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustname, ' ', 1), ' ', -1)
    ELSE NULL
  END AS p2,
  CASE
    WHEN CHAR_LENGTH(sbcustemail) = 0
    THEN NULL
    WHEN CHAR_LENGTH('.') = 0
    THEN CASE
      WHEN ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)) = 1
      THEN sbcustemail
      ELSE NULL
    END
    WHEN (
      CHAR_LENGTH(sbcustemail) - CHAR_LENGTH(REPLACE(sbcustemail, '.', ''))
    ) / CHAR_LENGTH('.') + 1 >= TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustemail, '.', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      '.',
      -1
    )
    WHEN (
      CHAR_LENGTH(sbcustemail) - CHAR_LENGTH(REPLACE(sbcustemail, '.', ''))
    ) / CHAR_LENGTH('.') + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustemail, '.', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      '.',
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustemail, '.', 1), '.', -1)
    ELSE NULL
  END AS p3,
  CASE
    WHEN CHAR_LENGTH(sbcustemail) = 0
    THEN NULL
    WHEN CHAR_LENGTH('.') = 0
    THEN CASE
      WHEN ABS((
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )) = 1
      THEN sbcustemail
      ELSE NULL
    END
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) <= (
      CHAR_LENGTH(sbcustemail) - CHAR_LENGTH(REPLACE(sbcustemail, '.', ''))
    ) / CHAR_LENGTH('.') + 1
    AND (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustemail, '.', (
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )),
      '.',
      -1
    )
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) < 0
    AND (
      CHAR_LENGTH(sbcustemail) - CHAR_LENGTH(REPLACE(sbcustemail, '.', ''))
    ) / CHAR_LENGTH('.') + 1 >= ABS((
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ))
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustemail, '.', (
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )),
      '.',
      1
    )
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustemail, '.', 1), '.', -1)
    ELSE NULL
  END AS p4,
  CASE
    WHEN CHAR_LENGTH(sbcustphone) = 0
    THEN NULL
    WHEN CHAR_LENGTH('-') = 0
    THEN CASE
      WHEN ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)) = 1
      THEN sbcustphone
      ELSE NULL
    END
    WHEN (
      CHAR_LENGTH(sbcustphone) - CHAR_LENGTH(REPLACE(sbcustphone, '-', ''))
    ) / CHAR_LENGTH('-') + 1 >= TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustphone, '-', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      '-',
      -1
    )
    WHEN (
      CHAR_LENGTH(sbcustphone) - CHAR_LENGTH(REPLACE(sbcustphone, '-', ''))
    ) / CHAR_LENGTH('-') + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustphone, '-', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      '-',
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustphone, '-', 1), '-', -1)
    ELSE NULL
  END AS p5,
  CASE
    WHEN CHAR_LENGTH(sbcustphone) = 0
    THEN NULL
    WHEN CHAR_LENGTH('-') = 0
    THEN CASE
      WHEN ABS((
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )) = 1
      THEN sbcustphone
      ELSE NULL
    END
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) <= (
      CHAR_LENGTH(sbcustphone) - CHAR_LENGTH(REPLACE(sbcustphone, '-', ''))
    ) / CHAR_LENGTH('-') + 1
    AND (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustphone, '-', (
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )),
      '-',
      -1
    )
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) < 0
    AND (
      CHAR_LENGTH(sbcustphone) - CHAR_LENGTH(REPLACE(sbcustphone, '-', ''))
    ) / CHAR_LENGTH('-') + 1 >= ABS((
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ))
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustphone, '-', (
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )),
      '-',
      1
    )
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustphone, '-', 1), '-', -1)
    ELSE NULL
  END AS p6,
  CASE
    WHEN CHAR_LENGTH(sbcustpostalcode) = 0
    THEN NULL
    WHEN CHAR_LENGTH('00') = 0
    THEN CASE
      WHEN ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)) = 1
      THEN sbcustpostalcode
      ELSE NULL
    END
    WHEN (
      CHAR_LENGTH(sbcustpostalcode) - CHAR_LENGTH(REPLACE(sbcustpostalcode, '00', ''))
    ) / CHAR_LENGTH('00') + 1 >= TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustpostalcode, '00', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      '00',
      -1
    )
    WHEN (
      CHAR_LENGTH(sbcustpostalcode) - CHAR_LENGTH(REPLACE(sbcustpostalcode, '00', ''))
    ) / CHAR_LENGTH('00') + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustpostalcode, '00', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      '00',
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustpostalcode, '00', 1), '00', -1)
    ELSE NULL
  END AS p7,
  CASE
    WHEN CHAR_LENGTH(sbcustpostalcode) = 0
    THEN NULL
    WHEN CHAR_LENGTH('00') = 0
    THEN CASE
      WHEN ABS((
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )) = 1
      THEN sbcustpostalcode
      ELSE NULL
    END
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) <= (
      CHAR_LENGTH(sbcustpostalcode) - CHAR_LENGTH(REPLACE(sbcustpostalcode, '00', ''))
    ) / CHAR_LENGTH('00') + 1
    AND (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustpostalcode, '00', (
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )),
      '00',
      -1
    )
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) < 0
    AND (
      CHAR_LENGTH(sbcustpostalcode) - CHAR_LENGTH(REPLACE(sbcustpostalcode, '00', ''))
    ) / CHAR_LENGTH('00') + 1 >= ABS((
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ))
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustpostalcode, '00', (
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )),
      '00',
      1
    )
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustpostalcode, '00', 1), '00', -1)
    ELSE NULL
  END AS p8,
  CASE
    WHEN CHAR_LENGTH(sbcustname) = 0
    THEN NULL
    WHEN CHAR_LENGTH('!') = 0
    THEN CASE
      WHEN ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)) = 1
      THEN sbcustname
      ELSE NULL
    END
    WHEN (
      CHAR_LENGTH(sbcustname) - CHAR_LENGTH(REPLACE(sbcustname, '!', ''))
    ) / CHAR_LENGTH('!') + 1 >= TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustname, '!', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      '!',
      -1
    )
    WHEN (
      CHAR_LENGTH(sbcustname) - CHAR_LENGTH(REPLACE(sbcustname, '!', ''))
    ) / CHAR_LENGTH('!') + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustname, '!', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      '!',
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustname, '!', 1), '!', -1)
    ELSE NULL
  END AS p9,
  CASE
    WHEN CHAR_LENGTH(sbcustname) = 0
    THEN NULL
    WHEN CHAR_LENGTH('@') = 0
    THEN CASE
      WHEN ABS((
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )) = 1
      THEN sbcustname
      ELSE NULL
    END
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) <= (
      CHAR_LENGTH(sbcustname) - CHAR_LENGTH(REPLACE(sbcustname, '@', ''))
    ) / CHAR_LENGTH('@') + 1
    AND (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustname, '@', (
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )),
      '@',
      -1
    )
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) < 0
    AND (
      CHAR_LENGTH(sbcustname) - CHAR_LENGTH(REPLACE(sbcustname, '@', ''))
    ) / CHAR_LENGTH('@') + 1 >= ABS((
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ))
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustname, '@', (
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )),
      '@',
      1
    )
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustname, '@', 1), '@', -1)
    ELSE NULL
  END AS p10,
  CASE
    WHEN CHAR_LENGTH(sbcustname) = 0
    THEN NULL
    WHEN CHAR_LENGTH('aa') = 0
    THEN CASE
      WHEN ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)) = 1
      THEN sbcustname
      ELSE NULL
    END
    WHEN (
      CHAR_LENGTH(sbcustname) - CHAR_LENGTH(REPLACE(sbcustname, 'aa', ''))
    ) / CHAR_LENGTH('aa') + 1 >= TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustname, 'aa', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      'aa',
      -1
    )
    WHEN (
      CHAR_LENGTH(sbcustname) - CHAR_LENGTH(REPLACE(sbcustname, 'aa', ''))
    ) / CHAR_LENGTH('aa') + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustname, 'aa', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      'aa',
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustname, 'aa', 1), 'aa', -1)
    ELSE NULL
  END AS p11,
  CASE
    WHEN CHAR_LENGTH(sbcustname) = 0
    THEN NULL
    WHEN CHAR_LENGTH('#$*') = 0
    THEN CASE
      WHEN ABS((
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )) = 1
      THEN sbcustname
      ELSE NULL
    END
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) <= (
      CHAR_LENGTH(sbcustname) - CHAR_LENGTH(REPLACE(sbcustname, '#$*', ''))
    ) / CHAR_LENGTH('#$*') + 1
    AND (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustname, '#$*', (
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )),
      '#$*',
      -1
    )
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) < 0
    AND (
      CHAR_LENGTH(sbcustname) - CHAR_LENGTH(REPLACE(sbcustname, '#$*', ''))
    ) / CHAR_LENGTH('#$*') + 1 >= ABS((
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ))
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustname, '#$*', (
        0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      )),
      '#$*',
      1
    )
    WHEN (
      0 - TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    ) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustname, '#$*', 1), '#$*', -1)
    ELSE NULL
  END AS p12,
  CASE
    WHEN CHAR_LENGTH(sbcustname) = 0
    THEN NULL
    WHEN CHAR_LENGTH('') = 0
    THEN CASE
      WHEN ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)) = 1
      THEN sbcustname
      ELSE NULL
    END
    WHEN (
      CHAR_LENGTH(sbcustname) - CHAR_LENGTH(REPLACE(sbcustname, '', ''))
    ) / CHAR_LENGTH('') + 1 >= TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustname, '', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      '',
      -1
    )
    WHEN (
      CHAR_LENGTH(sbcustname) - CHAR_LENGTH(REPLACE(sbcustname, '', ''))
    ) / CHAR_LENGTH('') + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustname, '', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      '',
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustname, '', 1), '', -1)
    ELSE NULL
  END AS p13,
  CASE
    WHEN CHAR_LENGTH('') = 0
    THEN NULL
    WHEN CHAR_LENGTH(' ') = 0
    THEN CASE
      WHEN ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)) = 1
      THEN ''
      ELSE NULL
    END
    WHEN (
      CHAR_LENGTH('') - CHAR_LENGTH(REPLACE('', ' ', ''))
    ) / CHAR_LENGTH(' ') + 1 >= TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX('', ' ', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      ' ',
      -1
    )
    WHEN (
      CHAR_LENGTH('') - CHAR_LENGTH(REPLACE('', ' ', ''))
    ) / CHAR_LENGTH(' ') + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX('', ' ', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)), ' ', 1)
    WHEN TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX('', ' ', 1), ' ', -1)
    ELSE NULL
  END AS p14,
  SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustname, ' ', 1), ' ', -1) AS p15,
  CASE
    WHEN CHAR_LENGTH(sbcuststate) = 0
    THEN NULL
    WHEN CHAR_LENGTH(sbcuststate) = 0
    THEN CASE
      WHEN ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)) = 1
      THEN sbcuststate
      ELSE NULL
    END
    WHEN (
      CHAR_LENGTH(sbcuststate) - CHAR_LENGTH(REPLACE(sbcuststate, sbcuststate, ''))
    ) / CHAR_LENGTH(sbcuststate) + 1 >= TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcuststate, sbcuststate, TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      sbcuststate,
      -1
    )
    WHEN (
      CHAR_LENGTH(sbcuststate) - CHAR_LENGTH(REPLACE(sbcuststate, sbcuststate, ''))
    ) / CHAR_LENGTH(sbcuststate) + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcuststate, sbcuststate, TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      sbcuststate,
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcuststate, sbcuststate, 1), sbcuststate, -1)
    ELSE NULL
  END AS p16,
  CASE
    WHEN CHAR_LENGTH(
      CASE
        WHEN CHAR_LENGTH(sbcustphone) = 0
        THEN NULL
        WHEN CHAR_LENGTH('-') = 0
        THEN CASE WHEN ABS(1) = 1 THEN sbcustphone ELSE NULL END
        WHEN (
          CHAR_LENGTH(sbcustphone) - CHAR_LENGTH(REPLACE(sbcustphone, '-', ''))
        ) / CHAR_LENGTH('-') >= 0
        THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustphone, '-', 1), '-', -1)
        ELSE NULL
      END
    ) = 0
    THEN NULL
    WHEN CHAR_LENGTH('5') = 0
    THEN CASE
      WHEN ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)) = 1
      THEN CASE
        WHEN CHAR_LENGTH(sbcustphone) = 0
        THEN NULL
        WHEN CHAR_LENGTH('-') = 0
        THEN CASE WHEN ABS(1) = 1 THEN sbcustphone ELSE NULL END
        WHEN (
          CHAR_LENGTH(sbcustphone) - CHAR_LENGTH(REPLACE(sbcustphone, '-', ''))
        ) / CHAR_LENGTH('-') >= 0
        THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustphone, '-', 1), '-', -1)
        ELSE NULL
      END
      ELSE NULL
    END
    WHEN (
      CHAR_LENGTH(
        CASE
          WHEN CHAR_LENGTH(sbcustphone) = 0
          THEN NULL
          WHEN CHAR_LENGTH('-') = 0
          THEN CASE WHEN ABS(1) = 1 THEN sbcustphone ELSE NULL END
          WHEN (
            CHAR_LENGTH(sbcustphone) - CHAR_LENGTH(REPLACE(sbcustphone, '-', ''))
          ) / CHAR_LENGTH('-') >= 0
          THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustphone, '-', 1), '-', -1)
          ELSE NULL
        END
      ) - CHAR_LENGTH(
        REPLACE(
          CASE
            WHEN CHAR_LENGTH(sbcustphone) = 0
            THEN NULL
            WHEN CHAR_LENGTH('-') = 0
            THEN CASE WHEN ABS(1) = 1 THEN sbcustphone ELSE NULL END
            WHEN (
              CHAR_LENGTH(sbcustphone) - CHAR_LENGTH(REPLACE(sbcustphone, '-', ''))
            ) / CHAR_LENGTH('-') >= 0
            THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustphone, '-', 1), '-', -1)
            ELSE NULL
          END,
          '5',
          ''
        )
      )
    ) / CHAR_LENGTH('5') + 1 >= TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(
        CASE
          WHEN CHAR_LENGTH(sbcustphone) = 0
          THEN NULL
          WHEN CHAR_LENGTH('-') = 0
          THEN CASE WHEN ABS(1) = 1 THEN sbcustphone ELSE NULL END
          WHEN (
            CHAR_LENGTH(sbcustphone) - CHAR_LENGTH(REPLACE(sbcustphone, '-', ''))
          ) / CHAR_LENGTH('-') >= 0
          THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustphone, '-', 1), '-', -1)
          ELSE NULL
        END,
        '5',
        TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      ),
      '5',
      -1
    )
    WHEN (
      CHAR_LENGTH(
        CASE
          WHEN CHAR_LENGTH(sbcustphone) = 0
          THEN NULL
          WHEN CHAR_LENGTH('-') = 0
          THEN CASE WHEN ABS(1) = 1 THEN sbcustphone ELSE NULL END
          WHEN (
            CHAR_LENGTH(sbcustphone) - CHAR_LENGTH(REPLACE(sbcustphone, '-', ''))
          ) / CHAR_LENGTH('-') >= 0
          THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustphone, '-', 1), '-', -1)
          ELSE NULL
        END
      ) - CHAR_LENGTH(
        REPLACE(
          CASE
            WHEN CHAR_LENGTH(sbcustphone) = 0
            THEN NULL
            WHEN CHAR_LENGTH('-') = 0
            THEN CASE WHEN ABS(1) = 1 THEN sbcustphone ELSE NULL END
            WHEN (
              CHAR_LENGTH(sbcustphone) - CHAR_LENGTH(REPLACE(sbcustphone, '-', ''))
            ) / CHAR_LENGTH('-') >= 0
            THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustphone, '-', 1), '-', -1)
            ELSE NULL
          END,
          '5',
          ''
        )
      )
    ) / CHAR_LENGTH('5') + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(
        CASE
          WHEN CHAR_LENGTH(sbcustphone) = 0
          THEN NULL
          WHEN CHAR_LENGTH('-') = 0
          THEN CASE WHEN ABS(1) = 1 THEN sbcustphone ELSE NULL END
          WHEN (
            CHAR_LENGTH(sbcustphone) - CHAR_LENGTH(REPLACE(sbcustphone, '-', ''))
          ) / CHAR_LENGTH('-') >= 0
          THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustphone, '-', 1), '-', -1)
          ELSE NULL
        END,
        '5',
        TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
      ),
      '5',
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(
        CASE
          WHEN CHAR_LENGTH(sbcustphone) = 0
          THEN NULL
          WHEN CHAR_LENGTH('-') = 0
          THEN CASE WHEN ABS(1) = 1 THEN sbcustphone ELSE NULL END
          WHEN (
            CHAR_LENGTH(sbcustphone) - CHAR_LENGTH(REPLACE(sbcustphone, '-', ''))
          ) / CHAR_LENGTH('-') >= 0
          THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustphone, '-', 1), '-', -1)
          ELSE NULL
        END,
        '5',
        1
      ),
      '5',
      -1
    )
    ELSE NULL
  END AS p17,
  CASE
    WHEN CHAR_LENGTH(sbcustpostalcode) = 0
    THEN NULL
    WHEN CHAR_LENGTH('0') = 0
    THEN CASE
      WHEN ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)) = 1
      THEN sbcustpostalcode
      ELSE NULL
    END
    WHEN (
      CHAR_LENGTH(sbcustpostalcode) - CHAR_LENGTH(REPLACE(sbcustpostalcode, '0', ''))
    ) / CHAR_LENGTH('0') + 1 >= TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustpostalcode, '0', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      '0',
      -1
    )
    WHEN (
      CHAR_LENGTH(sbcustpostalcode) - CHAR_LENGTH(REPLACE(sbcustpostalcode, '0', ''))
    ) / CHAR_LENGTH('0') + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbcustpostalcode, '0', TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0)),
      '0',
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustpostalcode, '0', 1), '0', -1)
    ELSE NULL
  END AS p18
FROM main.sbCustomer
WHERE
  TRUNCATE(CAST(SUBSTRING(sbcustid, 2) AS FLOAT), 0) <= 4
ORDER BY
  1
