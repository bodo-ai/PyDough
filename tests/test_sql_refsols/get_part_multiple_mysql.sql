SELECT
  TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) AS _expr0,
  CASE
    WHEN CHAR_LENGTH(sbCustName) = 0
    THEN ''
    WHEN (
      CHAR_LENGTH(sbCustName) - CHAR_LENGTH(REPLACE(sbCustName, ' ', ''))
    ) + 1 >= TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustName, ' ', TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)),
      ' ',
      -1
    )
    WHEN (
      CHAR_LENGTH(sbCustName) - CHAR_LENGTH(REPLACE(sbCustName, ' ', ''))
    ) + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustName, ' ', TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)),
      ' ',
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustName, ' ', 1), ' ', -1)
    ELSE ''
  END AS p1,
  CASE
    WHEN CHAR_LENGTH(sbCustName) = 0
    THEN ''
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) <= (
      CHAR_LENGTH(sbCustName) - CHAR_LENGTH(REPLACE(sbCustName, ' ', ''))
    ) + 1
    AND (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustName, ' ', (
        -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
      )),
      ' ',
      -1
    )
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) < 0
    AND (
      CHAR_LENGTH(sbCustName) - CHAR_LENGTH(REPLACE(sbCustName, ' ', ''))
    ) + 1 >= ABS((
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ))
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustName, ' ', (
        -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
      )),
      ' ',
      1
    )
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustName, ' ', 1), ' ', -1)
    ELSE ''
  END AS p2,
  CASE
    WHEN CHAR_LENGTH(sbCustEmail) = 0
    THEN ''
    WHEN (
      CHAR_LENGTH(sbCustEmail) - CHAR_LENGTH(REPLACE(sbCustEmail, '.', ''))
    ) + 1 >= TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustEmail, '.', TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)),
      '.',
      -1
    )
    WHEN (
      CHAR_LENGTH(sbCustEmail) - CHAR_LENGTH(REPLACE(sbCustEmail, '.', ''))
    ) + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustEmail, '.', TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)),
      '.',
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustEmail, '.', 1), '.', -1)
    ELSE ''
  END AS p3,
  CASE
    WHEN CHAR_LENGTH(sbCustEmail) = 0
    THEN ''
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) <= (
      CHAR_LENGTH(sbCustEmail) - CHAR_LENGTH(REPLACE(sbCustEmail, '.', ''))
    ) + 1
    AND (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustEmail, '.', (
        -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
      )),
      '.',
      -1
    )
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) < 0
    AND (
      CHAR_LENGTH(sbCustEmail) - CHAR_LENGTH(REPLACE(sbCustEmail, '.', ''))
    ) + 1 >= ABS((
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ))
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustEmail, '.', (
        -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
      )),
      '.',
      1
    )
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustEmail, '.', 1), '.', -1)
    ELSE ''
  END AS p4,
  CASE
    WHEN CHAR_LENGTH(sbCustPhone) = 0
    THEN ''
    WHEN (
      CHAR_LENGTH(sbCustPhone) - CHAR_LENGTH(REPLACE(sbCustPhone, '-', ''))
    ) + 1 >= TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustPhone, '-', TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)),
      '-',
      -1
    )
    WHEN (
      CHAR_LENGTH(sbCustPhone) - CHAR_LENGTH(REPLACE(sbCustPhone, '-', ''))
    ) + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustPhone, '-', TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)),
      '-',
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustPhone, '-', 1), '-', -1)
    ELSE ''
  END AS p5,
  CASE
    WHEN CHAR_LENGTH(sbCustPhone) = 0
    THEN ''
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) <= (
      CHAR_LENGTH(sbCustPhone) - CHAR_LENGTH(REPLACE(sbCustPhone, '-', ''))
    ) + 1
    AND (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustPhone, '-', (
        -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
      )),
      '-',
      -1
    )
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) < 0
    AND (
      CHAR_LENGTH(sbCustPhone) - CHAR_LENGTH(REPLACE(sbCustPhone, '-', ''))
    ) + 1 >= ABS((
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ))
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustPhone, '-', (
        -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
      )),
      '-',
      1
    )
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustPhone, '-', 1), '-', -1)
    ELSE ''
  END AS p6,
  CASE
    WHEN CHAR_LENGTH(sbCustPostalCode) = 0
    THEN ''
    WHEN (
      CHAR_LENGTH(sbCustPostalCode) - CHAR_LENGTH(REPLACE(sbCustPostalCode, '00', ''))
    ) / 2 + 1 >= TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustPostalCode, '00', TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)),
      '00',
      -1
    )
    WHEN (
      CHAR_LENGTH(sbCustPostalCode) - CHAR_LENGTH(REPLACE(sbCustPostalCode, '00', ''))
    ) / 2 + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustPostalCode, '00', TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)),
      '00',
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustPostalCode, '00', 1), '00', -1)
    ELSE ''
  END AS p7,
  CASE
    WHEN CHAR_LENGTH(sbCustPostalCode) = 0
    THEN ''
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) <= (
      CHAR_LENGTH(sbCustPostalCode) - CHAR_LENGTH(REPLACE(sbCustPostalCode, '00', ''))
    ) / 2 + 1
    AND (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(
        sbCustPostalCode,
        '00',
        (
          -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
        )
      ),
      '00',
      -1
    )
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) < 0
    AND (
      CHAR_LENGTH(sbCustPostalCode) - CHAR_LENGTH(REPLACE(sbCustPostalCode, '00', ''))
    ) / 2 + 1 >= ABS((
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ))
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(
        sbCustPostalCode,
        '00',
        (
          -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
        )
      ),
      '00',
      1
    )
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustPostalCode, '00', 1), '00', -1)
    ELSE ''
  END AS p8,
  CASE
    WHEN CHAR_LENGTH(sbCustName) = 0
    THEN ''
    WHEN (
      CHAR_LENGTH(sbCustName) - CHAR_LENGTH(REPLACE(sbCustName, '!', ''))
    ) + 1 >= TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustName, '!', TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)),
      '!',
      -1
    )
    WHEN (
      CHAR_LENGTH(sbCustName) - CHAR_LENGTH(REPLACE(sbCustName, '!', ''))
    ) + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustName, '!', TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)),
      '!',
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustName, '!', 1), '!', -1)
    ELSE ''
  END AS p9,
  CASE
    WHEN CHAR_LENGTH(sbCustName) = 0
    THEN ''
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) <= (
      CHAR_LENGTH(sbCustName) - CHAR_LENGTH(REPLACE(sbCustName, '@', ''))
    ) + 1
    AND (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustName, '@', (
        -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
      )),
      '@',
      -1
    )
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) < 0
    AND (
      CHAR_LENGTH(sbCustName) - CHAR_LENGTH(REPLACE(sbCustName, '@', ''))
    ) + 1 >= ABS((
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ))
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustName, '@', (
        -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
      )),
      '@',
      1
    )
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustName, '@', 1), '@', -1)
    ELSE ''
  END AS p10,
  CASE
    WHEN CHAR_LENGTH(sbCustName) = 0
    THEN ''
    WHEN (
      CHAR_LENGTH(sbCustName) - CHAR_LENGTH(REPLACE(sbCustName, 'aa', ''))
    ) / 2 + 1 >= TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustName, 'aa', TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)),
      'aa',
      -1
    )
    WHEN (
      CHAR_LENGTH(sbCustName) - CHAR_LENGTH(REPLACE(sbCustName, 'aa', ''))
    ) / 2 + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustName, 'aa', TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)),
      'aa',
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustName, 'aa', 1), 'aa', -1)
    ELSE ''
  END AS p11,
  CASE
    WHEN CHAR_LENGTH(sbCustName) = 0
    THEN ''
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) <= (
      CHAR_LENGTH(sbCustName) - CHAR_LENGTH(REPLACE(sbCustName, '#$*', ''))
    ) / 3 + 1
    AND (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustName, '#$*', (
        -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
      )),
      '#$*',
      -1
    )
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) < 0
    AND (
      CHAR_LENGTH(sbCustName) - CHAR_LENGTH(REPLACE(sbCustName, '#$*', ''))
    ) / 3 + 1 >= ABS((
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ))
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustName, '#$*', (
        -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
      )),
      '#$*',
      1
    )
    WHEN (
      -1 * TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    ) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustName, '#$*', 1), '#$*', -1)
    ELSE ''
  END AS p12,
  CASE
    WHEN ABS(TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)) = 1
    THEN sbCustName
    ELSE ''
  END AS p13,
  '' AS p14,
  SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustName, ' ', 1), ' ', -1) AS p15,
  CASE
    WHEN CHAR_LENGTH(sbCustState) = 0
    THEN ''
    WHEN CHAR_LENGTH(sbCustState) = 0
    THEN CASE
      WHEN ABS(TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)) = 1
      THEN sbCustState
      ELSE ''
    END
    WHEN (
      CHAR_LENGTH(sbCustState) - CHAR_LENGTH(REPLACE(sbCustState, sbCustState, ''))
    ) / CHAR_LENGTH(sbCustState) + 1 >= TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustState, sbCustState, TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)),
      sbCustState,
      -1
    )
    WHEN (
      CHAR_LENGTH(sbCustState) - CHAR_LENGTH(REPLACE(sbCustState, sbCustState, ''))
    ) / CHAR_LENGTH(sbCustState) + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustState, sbCustState, TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)),
      sbCustState,
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustState, sbCustState, 1), sbCustState, -1)
    ELSE ''
  END AS p16,
  CASE
    WHEN CHAR_LENGTH(
      CASE
        WHEN CHAR_LENGTH(sbCustPhone) = 0
        THEN ''
        WHEN (
          CHAR_LENGTH(sbCustPhone) - CHAR_LENGTH(REPLACE(sbCustPhone, '-', ''))
        ) >= 0
        THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustPhone, '-', 1), '-', -1)
        ELSE ''
      END
    ) = 0
    THEN ''
    WHEN (
      CHAR_LENGTH(
        CASE
          WHEN CHAR_LENGTH(sbCustPhone) = 0
          THEN ''
          WHEN (
            CHAR_LENGTH(sbCustPhone) - CHAR_LENGTH(REPLACE(sbCustPhone, '-', ''))
          ) >= 0
          THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustPhone, '-', 1), '-', -1)
          ELSE ''
        END
      ) - CHAR_LENGTH(
        REPLACE(
          CASE
            WHEN CHAR_LENGTH(sbCustPhone) = 0
            THEN ''
            WHEN (
              CHAR_LENGTH(sbCustPhone) - CHAR_LENGTH(REPLACE(sbCustPhone, '-', ''))
            ) >= 0
            THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustPhone, '-', 1), '-', -1)
            ELSE ''
          END,
          '5',
          ''
        )
      )
    ) + 1 >= TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(
        CASE
          WHEN CHAR_LENGTH(sbCustPhone) = 0
          THEN ''
          WHEN (
            CHAR_LENGTH(sbCustPhone) - CHAR_LENGTH(REPLACE(sbCustPhone, '-', ''))
          ) >= 0
          THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustPhone, '-', 1), '-', -1)
          ELSE ''
        END,
        '5',
        TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
      ),
      '5',
      -1
    )
    WHEN (
      CHAR_LENGTH(
        CASE
          WHEN CHAR_LENGTH(sbCustPhone) = 0
          THEN ''
          WHEN (
            CHAR_LENGTH(sbCustPhone) - CHAR_LENGTH(REPLACE(sbCustPhone, '-', ''))
          ) >= 0
          THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustPhone, '-', 1), '-', -1)
          ELSE ''
        END
      ) - CHAR_LENGTH(
        REPLACE(
          CASE
            WHEN CHAR_LENGTH(sbCustPhone) = 0
            THEN ''
            WHEN (
              CHAR_LENGTH(sbCustPhone) - CHAR_LENGTH(REPLACE(sbCustPhone, '-', ''))
            ) >= 0
            THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustPhone, '-', 1), '-', -1)
            ELSE ''
          END,
          '5',
          ''
        )
      )
    ) + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(
        CASE
          WHEN CHAR_LENGTH(sbCustPhone) = 0
          THEN ''
          WHEN (
            CHAR_LENGTH(sbCustPhone) - CHAR_LENGTH(REPLACE(sbCustPhone, '-', ''))
          ) >= 0
          THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustPhone, '-', 1), '-', -1)
          ELSE ''
        END,
        '5',
        TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
      ),
      '5',
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(
        CASE
          WHEN CHAR_LENGTH(sbCustPhone) = 0
          THEN ''
          WHEN (
            CHAR_LENGTH(sbCustPhone) - CHAR_LENGTH(REPLACE(sbCustPhone, '-', ''))
          ) >= 0
          THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustPhone, '-', 1), '-', -1)
          ELSE ''
        END,
        '5',
        1
      ),
      '5',
      -1
    )
    ELSE ''
  END AS p17,
  CASE
    WHEN CHAR_LENGTH(sbCustPostalCode) = 0
    THEN ''
    WHEN (
      CHAR_LENGTH(sbCustPostalCode) - CHAR_LENGTH(REPLACE(sbCustPostalCode, '0', ''))
    ) + 1 >= TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) > 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustPostalCode, '0', TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)),
      '0',
      -1
    )
    WHEN (
      CHAR_LENGTH(sbCustPostalCode) - CHAR_LENGTH(REPLACE(sbCustPostalCode, '0', ''))
    ) + 1 >= ABS(TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0))
    AND TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) < 0
    THEN SUBSTRING_INDEX(
      SUBSTRING_INDEX(sbCustPostalCode, '0', TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0)),
      '0',
      1
    )
    WHEN TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) = 0
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustPostalCode, '0', 1), '0', -1)
    ELSE ''
  END AS p18
FROM main.sbCustomer
WHERE
  TRUNCATE(CAST(SUBSTRING(sbCustId, 2) AS FLOAT), 0) <= 4
ORDER BY
  1
