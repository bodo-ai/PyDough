SELECT
  CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) AS _expr0,
  CASE
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustname, ' ', 1)
    WHEN -CAST(CAST((
      LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' ', ''))
    ) AS DOUBLE) AS BIGINT) > CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST((
      LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' ', ''))
    ) AS DOUBLE) AS BIGINT) < CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustname,
      ' ',
      CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' ', ''))
      ) AS DOUBLE) AS BIGINT) + CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) + 2
    )
    ELSE SPLIT_PART(sbcustname, ' ', CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT))
  END AS p1,
  CASE
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) = 0
    THEN SPLIT_PART(sbcustname, ' ', 1)
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) < -CAST(CAST((
      LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' ', ''))
    ) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) > CAST(CAST((
      LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' ', ''))
    ) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) < 0
    THEN SPLIT_PART(
      sbcustname,
      ' ',
      CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' ', ''))
      ) AS DOUBLE) AS BIGINT) + (
        0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
      ) + 2
    )
    ELSE SPLIT_PART(sbcustname, ' ', 0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT))
  END AS p2,
  CASE
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustemail, '.', 1)
    WHEN -CAST(CAST((
      LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.', ''))
    ) AS DOUBLE) AS BIGINT) > CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST((
      LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.', ''))
    ) AS DOUBLE) AS BIGINT) < CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustemail,
      '.',
      CAST(CAST((
        LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.', ''))
      ) AS DOUBLE) AS BIGINT) + CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) + 2
    )
    ELSE SPLIT_PART(sbcustemail, '.', CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT))
  END AS p3,
  CASE
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) = 0
    THEN SPLIT_PART(sbcustemail, '.', 1)
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) < -CAST(CAST((
      LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.', ''))
    ) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) > CAST(CAST((
      LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.', ''))
    ) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) < 0
    THEN SPLIT_PART(
      sbcustemail,
      '.',
      CAST(CAST((
        LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.', ''))
      ) AS DOUBLE) AS BIGINT) + (
        0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
      ) + 2
    )
    ELSE SPLIT_PART(sbcustemail, '.', 0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT))
  END AS p4,
  CASE
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustphone, '-', 1)
    WHEN -CAST(CAST((
      LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
    ) AS DOUBLE) AS BIGINT) > CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST((
      LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
    ) AS DOUBLE) AS BIGINT) < CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustphone,
      '-',
      CAST(CAST((
        LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
      ) AS DOUBLE) AS BIGINT) + CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) + 2
    )
    ELSE SPLIT_PART(sbcustphone, '-', CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT))
  END AS p5,
  CASE
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) = 0
    THEN SPLIT_PART(sbcustphone, '-', 1)
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) < -CAST(CAST((
      LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
    ) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) > CAST(CAST((
      LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
    ) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) < 0
    THEN SPLIT_PART(
      sbcustphone,
      '-',
      CAST(CAST((
        LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
      ) AS DOUBLE) AS BIGINT) + (
        0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
      ) + 2
    )
    ELSE SPLIT_PART(sbcustphone, '-', 0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT))
  END AS p6,
  CASE
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustpostalcode, '00', 1)
    WHEN -CAST(CAST((
      LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00', ''))
    ) AS DOUBLE) / 2 AS BIGINT) > CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST((
      LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00', ''))
    ) AS DOUBLE) / 2 AS BIGINT) < CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustpostalcode,
      '00',
      CAST(CAST((
        LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00', ''))
      ) AS DOUBLE) / 2 AS BIGINT) + CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) + 2
    )
    ELSE SPLIT_PART(sbcustpostalcode, '00', CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT))
  END AS p7,
  CASE
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) = 0
    THEN SPLIT_PART(sbcustpostalcode, '00', 1)
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) < -CAST(CAST((
      LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00', ''))
    ) AS DOUBLE) / 2 AS BIGINT)
    THEN NULL
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) > CAST(CAST((
      LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00', ''))
    ) AS DOUBLE) / 2 AS BIGINT)
    THEN NULL
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) < 0
    THEN SPLIT_PART(
      sbcustpostalcode,
      '00',
      CAST(CAST((
        LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00', ''))
      ) AS DOUBLE) / 2 AS BIGINT) + (
        0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
      ) + 2
    )
    ELSE SPLIT_PART(sbcustpostalcode, '00', 0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT))
  END AS p8,
  CASE
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustname, '!', 1)
    WHEN -CAST(CAST((
      LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '!', ''))
    ) AS DOUBLE) AS BIGINT) > CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST((
      LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '!', ''))
    ) AS DOUBLE) AS BIGINT) < CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustname,
      '!',
      CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '!', ''))
      ) AS DOUBLE) AS BIGINT) + CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) + 2
    )
    ELSE SPLIT_PART(sbcustname, '!', CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT))
  END AS p9,
  CASE
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) = 0
    THEN SPLIT_PART(sbcustname, '@', 1)
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) < -CAST(CAST((
      LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '@', ''))
    ) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) > CAST(CAST((
      LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '@', ''))
    ) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) < 0
    THEN SPLIT_PART(
      sbcustname,
      '@',
      CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '@', ''))
      ) AS DOUBLE) AS BIGINT) + (
        0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
      ) + 2
    )
    ELSE SPLIT_PART(sbcustname, '@', 0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT))
  END AS p10,
  CASE
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustname, 'aa', 1)
    WHEN -CAST(CAST((
      LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, 'aa', ''))
    ) AS DOUBLE) / 2 AS BIGINT) > CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST((
      LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, 'aa', ''))
    ) AS DOUBLE) / 2 AS BIGINT) < CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustname,
      'aa',
      CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, 'aa', ''))
      ) AS DOUBLE) / 2 AS BIGINT) + CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) + 2
    )
    ELSE SPLIT_PART(sbcustname, 'aa', CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT))
  END AS p11,
  CASE
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) = 0
    THEN SPLIT_PART(sbcustname, '#$*', 1)
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) < -CAST(CAST((
      LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '#$*', ''))
    ) AS DOUBLE) / 3 AS BIGINT)
    THEN NULL
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) > CAST(CAST((
      LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '#$*', ''))
    ) AS DOUBLE) / 3 AS BIGINT)
    THEN NULL
    WHEN (
      0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    ) < 0
    THEN SPLIT_PART(
      sbcustname,
      '#$*',
      CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '#$*', ''))
      ) AS DOUBLE) / 3 AS BIGINT) + (
        0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
      ) + 2
    )
    ELSE SPLIT_PART(sbcustname, '#$*', 0 - CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT))
  END AS p12,
  CASE
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustname, '', 1)
    WHEN -0 > CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) > 0
    THEN NULL
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) < 0
    THEN SPLIT_PART(sbcustname, '', 2 + CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT))
    ELSE SPLIT_PART(sbcustname, '', CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT))
  END AS p13,
  CASE
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) = 0
    THEN SPLIT_PART('', ' ', 1)
    WHEN -CAST(CAST((
      0 - LENGTH(REPLACE('', ' ', ''))
    ) AS DOUBLE) AS BIGINT) > CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST((
      0 - LENGTH(REPLACE('', ' ', ''))
    ) AS DOUBLE) AS BIGINT) < CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) < 0
    THEN SPLIT_PART(
      '',
      ' ',
      CAST(CAST((
        0 - LENGTH(REPLACE('', ' ', ''))
      ) AS DOUBLE) AS BIGINT) + CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) + 2
    )
    ELSE SPLIT_PART('', ' ', CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT))
  END AS p14,
  SPLIT_PART(sbcustname, ' ', 1) AS p15,
  CASE
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) = 0
    THEN SPLIT_PART(sbcuststate, sbcuststate, 1)
    WHEN -CASE
      WHEN LENGTH(sbcuststate) = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcuststate) - LENGTH(REPLACE(sbcuststate, sbcuststate, ''))
      ) AS DOUBLE) / LENGTH(sbcuststate) AS BIGINT)
    END > CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CASE
      WHEN LENGTH(sbcuststate) = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcuststate) - LENGTH(REPLACE(sbcuststate, sbcuststate, ''))
      ) AS DOUBLE) / LENGTH(sbcuststate) AS BIGINT)
    END < CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcuststate,
      sbcuststate,
      CASE
        WHEN LENGTH(sbcuststate) = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(sbcuststate) - LENGTH(REPLACE(sbcuststate, sbcuststate, ''))
        ) AS DOUBLE) / LENGTH(sbcuststate) AS BIGINT)
      END + CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) + 2
    )
    ELSE SPLIT_PART(sbcuststate, sbcuststate, CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT))
  END AS p16,
  CASE
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) = 0
    THEN SPLIT_PART(
      CASE
        WHEN -CAST(CAST((
          LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
        ) AS DOUBLE) AS BIGINT) > 1
        THEN NULL
        WHEN CAST(CAST((
          LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
        ) AS DOUBLE) AS BIGINT) < 1
        THEN NULL
        ELSE SPLIT_PART(sbcustphone, '-', 1)
      END,
      '5',
      1
    )
    WHEN -CAST(CAST((
      LENGTH(
        CASE
          WHEN -CAST(CAST((
            LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
          ) AS DOUBLE) AS BIGINT) > 1
          THEN NULL
          WHEN CAST(CAST((
            LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
          ) AS DOUBLE) AS BIGINT) < 1
          THEN NULL
          ELSE SPLIT_PART(sbcustphone, '-', 1)
        END
      ) - LENGTH(
        REPLACE(
          CASE
            WHEN -CAST(CAST((
              LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
            ) AS DOUBLE) AS BIGINT) > 1
            THEN NULL
            WHEN CAST(CAST((
              LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
            ) AS DOUBLE) AS BIGINT) < 1
            THEN NULL
            ELSE SPLIT_PART(sbcustphone, '-', 1)
          END,
          '5',
          ''
        )
      )
    ) AS DOUBLE) AS BIGINT) > CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST((
      LENGTH(
        CASE
          WHEN -CAST(CAST((
            LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
          ) AS DOUBLE) AS BIGINT) > 1
          THEN NULL
          WHEN CAST(CAST((
            LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
          ) AS DOUBLE) AS BIGINT) < 1
          THEN NULL
          ELSE SPLIT_PART(sbcustphone, '-', 1)
        END
      ) - LENGTH(
        REPLACE(
          CASE
            WHEN -CAST(CAST((
              LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
            ) AS DOUBLE) AS BIGINT) > 1
            THEN NULL
            WHEN CAST(CAST((
              LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
            ) AS DOUBLE) AS BIGINT) < 1
            THEN NULL
            ELSE SPLIT_PART(sbcustphone, '-', 1)
          END,
          '5',
          ''
        )
      )
    ) AS DOUBLE) AS BIGINT) < CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) < 0
    THEN SPLIT_PART(
      CASE
        WHEN -CAST(CAST((
          LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
        ) AS DOUBLE) AS BIGINT) > 1
        THEN NULL
        WHEN CAST(CAST((
          LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
        ) AS DOUBLE) AS BIGINT) < 1
        THEN NULL
        ELSE SPLIT_PART(sbcustphone, '-', 1)
      END,
      '5',
      CAST(CAST((
        LENGTH(
          CASE
            WHEN -CAST(CAST((
              LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
            ) AS DOUBLE) AS BIGINT) > 1
            THEN NULL
            WHEN CAST(CAST((
              LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
            ) AS DOUBLE) AS BIGINT) < 1
            THEN NULL
            ELSE SPLIT_PART(sbcustphone, '-', 1)
          END
        ) - LENGTH(
          REPLACE(
            CASE
              WHEN -CAST(CAST((
                LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
              ) AS DOUBLE) AS BIGINT) > 1
              THEN NULL
              WHEN CAST(CAST((
                LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
              ) AS DOUBLE) AS BIGINT) < 1
              THEN NULL
              ELSE SPLIT_PART(sbcustphone, '-', 1)
            END,
            '5',
            ''
          )
        )
      ) AS DOUBLE) AS BIGINT) + CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) + 2
    )
    ELSE SPLIT_PART(
      CASE
        WHEN -CAST(CAST((
          LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
        ) AS DOUBLE) AS BIGINT) > 1
        THEN NULL
        WHEN CAST(CAST((
          LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
        ) AS DOUBLE) AS BIGINT) < 1
        THEN NULL
        ELSE SPLIT_PART(sbcustphone, '-', 1)
      END,
      '5',
      CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    )
  END AS p17,
  CASE
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustpostalcode, '0', 1)
    WHEN -CAST(CAST((
      LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '0', ''))
    ) AS DOUBLE) AS BIGINT) > CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST((
      LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '0', ''))
    ) AS DOUBLE) AS BIGINT) < CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT)
    THEN NULL
    WHEN CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustpostalcode,
      '0',
      CAST(CAST((
        LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '0', ''))
      ) AS DOUBLE) AS BIGINT) + CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) + 2
    )
    ELSE SPLIT_PART(sbcustpostalcode, '0', CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT))
  END AS p18
FROM main.sbcustomer
WHERE
  CAST(CAST(SUBSTRING(sbcustid, 2) AS DOUBLE) AS BIGINT) <= 4
ORDER BY
  1 NULLS FIRST
