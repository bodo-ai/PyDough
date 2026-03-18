SELECT
  CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS _expr0,
  CASE
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustname, ' ', 1)
    WHEN -CASE
      WHEN LENGTH(' ') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' ', ''))
      ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
    END > CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CASE
      WHEN LENGTH(' ') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' ', ''))
      ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
    END < CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustname,
      ' ',
      CASE
        WHEN LENGTH(' ') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' ', ''))
        ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
      END - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) + 1
    )
    ELSE SPLIT_PART(sbcustname, ' ', CAST(SUBSTRING(sbcustid, 2) AS BIGINT))
  END AS p1,
  CASE
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustname, ' ', 1)
    WHEN -CASE
      WHEN LENGTH(' ') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' ', ''))
      ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
    END > 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) > CASE
      WHEN LENGTH(' ') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' ', ''))
      ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
    END
    THEN NULL
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustname,
      ' ',
      CASE
        WHEN LENGTH(' ') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' ', ''))
        ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
      END - 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) + 1
    )
    ELSE SPLIT_PART(sbcustname, ' ', 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT))
  END AS p2,
  CASE
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustemail, '.', 1)
    WHEN -CASE
      WHEN LENGTH('.') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.', ''))
      ) AS DOUBLE) / LENGTH('.') AS BIGINT)
    END > CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CASE
      WHEN LENGTH('.') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.', ''))
      ) AS DOUBLE) / LENGTH('.') AS BIGINT)
    END < CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustemail,
      '.',
      CASE
        WHEN LENGTH('.') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.', ''))
        ) AS DOUBLE) / LENGTH('.') AS BIGINT)
      END - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) + 1
    )
    ELSE SPLIT_PART(sbcustemail, '.', CAST(SUBSTRING(sbcustid, 2) AS BIGINT))
  END AS p3,
  CASE
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustemail, '.', 1)
    WHEN -CASE
      WHEN LENGTH('.') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.', ''))
      ) AS DOUBLE) / LENGTH('.') AS BIGINT)
    END > 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) > CASE
      WHEN LENGTH('.') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.', ''))
      ) AS DOUBLE) / LENGTH('.') AS BIGINT)
    END
    THEN NULL
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustemail,
      '.',
      CASE
        WHEN LENGTH('.') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(sbcustemail) - LENGTH(REPLACE(sbcustemail, '.', ''))
        ) AS DOUBLE) / LENGTH('.') AS BIGINT)
      END - 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) + 1
    )
    ELSE SPLIT_PART(sbcustemail, '.', 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT))
  END AS p4,
  CASE
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustphone, '-', 1)
    WHEN -CASE
      WHEN LENGTH('-') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
      ) AS DOUBLE) / LENGTH('-') AS BIGINT)
    END > CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CASE
      WHEN LENGTH('-') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
      ) AS DOUBLE) / LENGTH('-') AS BIGINT)
    END < CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustphone,
      '-',
      CASE
        WHEN LENGTH('-') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
        ) AS DOUBLE) / LENGTH('-') AS BIGINT)
      END - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) + 1
    )
    ELSE SPLIT_PART(sbcustphone, '-', CAST(SUBSTRING(sbcustid, 2) AS BIGINT))
  END AS p5,
  CASE
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustphone, '-', 1)
    WHEN -CASE
      WHEN LENGTH('-') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
      ) AS DOUBLE) / LENGTH('-') AS BIGINT)
    END > 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) > CASE
      WHEN LENGTH('-') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
      ) AS DOUBLE) / LENGTH('-') AS BIGINT)
    END
    THEN NULL
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustphone,
      '-',
      CASE
        WHEN LENGTH('-') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
        ) AS DOUBLE) / LENGTH('-') AS BIGINT)
      END - 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) + 1
    )
    ELSE SPLIT_PART(sbcustphone, '-', 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT))
  END AS p6,
  CASE
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustpostalcode, '00', 1)
    WHEN -CASE
      WHEN LENGTH('00') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00', ''))
      ) AS DOUBLE) / LENGTH('00') AS BIGINT)
    END > CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CASE
      WHEN LENGTH('00') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00', ''))
      ) AS DOUBLE) / LENGTH('00') AS BIGINT)
    END < CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustpostalcode,
      '00',
      CASE
        WHEN LENGTH('00') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00', ''))
        ) AS DOUBLE) / LENGTH('00') AS BIGINT)
      END - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) + 1
    )
    ELSE SPLIT_PART(sbcustpostalcode, '00', CAST(SUBSTRING(sbcustid, 2) AS BIGINT))
  END AS p7,
  CASE
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustpostalcode, '00', 1)
    WHEN -CASE
      WHEN LENGTH('00') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00', ''))
      ) AS DOUBLE) / LENGTH('00') AS BIGINT)
    END > 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) > CASE
      WHEN LENGTH('00') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00', ''))
      ) AS DOUBLE) / LENGTH('00') AS BIGINT)
    END
    THEN NULL
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustpostalcode,
      '00',
      CASE
        WHEN LENGTH('00') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '00', ''))
        ) AS DOUBLE) / LENGTH('00') AS BIGINT)
      END - 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) + 1
    )
    ELSE SPLIT_PART(sbcustpostalcode, '00', 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT))
  END AS p8,
  CASE
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustname, '!', 1)
    WHEN -CASE
      WHEN LENGTH('!') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '!', ''))
      ) AS DOUBLE) / LENGTH('!') AS BIGINT)
    END > CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CASE
      WHEN LENGTH('!') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '!', ''))
      ) AS DOUBLE) / LENGTH('!') AS BIGINT)
    END < CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustname,
      '!',
      CASE
        WHEN LENGTH('!') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '!', ''))
        ) AS DOUBLE) / LENGTH('!') AS BIGINT)
      END - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) + 1
    )
    ELSE SPLIT_PART(sbcustname, '!', CAST(SUBSTRING(sbcustid, 2) AS BIGINT))
  END AS p9,
  CASE
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustname, '@', 1)
    WHEN -CASE
      WHEN LENGTH('@') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '@', ''))
      ) AS DOUBLE) / LENGTH('@') AS BIGINT)
    END > 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) > CASE
      WHEN LENGTH('@') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '@', ''))
      ) AS DOUBLE) / LENGTH('@') AS BIGINT)
    END
    THEN NULL
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustname,
      '@',
      CASE
        WHEN LENGTH('@') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '@', ''))
        ) AS DOUBLE) / LENGTH('@') AS BIGINT)
      END - 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) + 1
    )
    ELSE SPLIT_PART(sbcustname, '@', 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT))
  END AS p10,
  CASE
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustname, 'aa', 1)
    WHEN -CASE
      WHEN LENGTH('aa') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, 'aa', ''))
      ) AS DOUBLE) / LENGTH('aa') AS BIGINT)
    END > CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CASE
      WHEN LENGTH('aa') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, 'aa', ''))
      ) AS DOUBLE) / LENGTH('aa') AS BIGINT)
    END < CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustname,
      'aa',
      CASE
        WHEN LENGTH('aa') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, 'aa', ''))
        ) AS DOUBLE) / LENGTH('aa') AS BIGINT)
      END - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) + 1
    )
    ELSE SPLIT_PART(sbcustname, 'aa', CAST(SUBSTRING(sbcustid, 2) AS BIGINT))
  END AS p11,
  CASE
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustname, '#$*', 1)
    WHEN -CASE
      WHEN LENGTH('#$*') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '#$*', ''))
      ) AS DOUBLE) / LENGTH('#$*') AS BIGINT)
    END > 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) > CASE
      WHEN LENGTH('#$*') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '#$*', ''))
      ) AS DOUBLE) / LENGTH('#$*') AS BIGINT)
    END
    THEN NULL
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustname,
      '#$*',
      CASE
        WHEN LENGTH('#$*') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '#$*', ''))
        ) AS DOUBLE) / LENGTH('#$*') AS BIGINT)
      END - 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) + 1
    )
    ELSE SPLIT_PART(sbcustname, '#$*', 0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT))
  END AS p12,
  CASE
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustname, '', 1)
    WHEN -CASE
      WHEN LENGTH('') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '', ''))
      ) AS DOUBLE) / LENGTH('') AS BIGINT)
    END > CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CASE
      WHEN LENGTH('') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '', ''))
      ) AS DOUBLE) / LENGTH('') AS BIGINT)
    END < CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustname,
      '',
      CASE
        WHEN LENGTH('') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, '', ''))
        ) AS DOUBLE) / LENGTH('') AS BIGINT)
      END - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) + 1
    )
    ELSE SPLIT_PART(sbcustname, '', CAST(SUBSTRING(sbcustid, 2) AS BIGINT))
  END AS p13,
  CASE
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) = 0
    THEN SPLIT_PART('', ' ', 1)
    WHEN -CASE
      WHEN LENGTH(' ') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH('') - LENGTH(REPLACE('', ' ', ''))
      ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
    END > CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CASE
      WHEN LENGTH(' ') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH('') - LENGTH(REPLACE('', ' ', ''))
      ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
    END < CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) < 0
    THEN SPLIT_PART(
      '',
      ' ',
      CASE
        WHEN LENGTH(' ') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH('') - LENGTH(REPLACE('', ' ', ''))
        ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
      END - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) + 1
    )
    ELSE SPLIT_PART('', ' ', CAST(SUBSTRING(sbcustid, 2) AS BIGINT))
  END AS p14,
  SPLIT_PART(sbcustname, ' ', 1) AS p15,
  CASE
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) = 0
    THEN SPLIT_PART(sbcuststate, sbcuststate, 1)
    WHEN -CASE
      WHEN LENGTH(sbcuststate) = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcuststate) - LENGTH(REPLACE(sbcuststate, sbcuststate, ''))
      ) AS DOUBLE) / LENGTH(sbcuststate) AS BIGINT)
    END > CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CASE
      WHEN LENGTH(sbcuststate) = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcuststate) - LENGTH(REPLACE(sbcuststate, sbcuststate, ''))
      ) AS DOUBLE) / LENGTH(sbcuststate) AS BIGINT)
    END < CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcuststate,
      sbcuststate,
      CASE
        WHEN LENGTH(sbcuststate) = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(sbcuststate) - LENGTH(REPLACE(sbcuststate, sbcuststate, ''))
        ) AS DOUBLE) / LENGTH(sbcuststate) AS BIGINT)
      END - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) + 1
    )
    ELSE SPLIT_PART(sbcuststate, sbcuststate, CAST(SUBSTRING(sbcustid, 2) AS BIGINT))
  END AS p16,
  CASE
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) = 0
    THEN SPLIT_PART(
      CASE
        WHEN -CASE
          WHEN LENGTH('-') = 0
          THEN 0
          ELSE CAST(CAST((
            LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
          ) AS DOUBLE) / LENGTH('-') AS BIGINT)
        END > 1
        THEN NULL
        WHEN CASE
          WHEN LENGTH('-') = 0
          THEN 0
          ELSE CAST(CAST((
            LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
          ) AS DOUBLE) / LENGTH('-') AS BIGINT)
        END < 1
        THEN NULL
        ELSE SPLIT_PART(sbcustphone, '-', 1)
      END,
      '5',
      1
    )
    WHEN -CASE
      WHEN LENGTH('5') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(
          CASE
            WHEN -CASE
              WHEN LENGTH('-') = 0
              THEN 0
              ELSE CAST(CAST((
                LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
              ) AS DOUBLE) / LENGTH('-') AS BIGINT)
            END > 1
            THEN NULL
            WHEN CASE
              WHEN LENGTH('-') = 0
              THEN 0
              ELSE CAST(CAST((
                LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
              ) AS DOUBLE) / LENGTH('-') AS BIGINT)
            END < 1
            THEN NULL
            ELSE SPLIT_PART(sbcustphone, '-', 1)
          END
        ) - LENGTH(
          REPLACE(
            CASE
              WHEN -CASE
                WHEN LENGTH('-') = 0
                THEN 0
                ELSE CAST(CAST((
                  LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
                ) AS DOUBLE) / LENGTH('-') AS BIGINT)
              END > 1
              THEN NULL
              WHEN CASE
                WHEN LENGTH('-') = 0
                THEN 0
                ELSE CAST(CAST((
                  LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
                ) AS DOUBLE) / LENGTH('-') AS BIGINT)
              END < 1
              THEN NULL
              ELSE SPLIT_PART(sbcustphone, '-', 1)
            END,
            '5',
            ''
          )
        )
      ) AS DOUBLE) / LENGTH('5') AS BIGINT)
    END > CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CASE
      WHEN LENGTH('5') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(
          CASE
            WHEN -CASE
              WHEN LENGTH('-') = 0
              THEN 0
              ELSE CAST(CAST((
                LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
              ) AS DOUBLE) / LENGTH('-') AS BIGINT)
            END > 1
            THEN NULL
            WHEN CASE
              WHEN LENGTH('-') = 0
              THEN 0
              ELSE CAST(CAST((
                LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
              ) AS DOUBLE) / LENGTH('-') AS BIGINT)
            END < 1
            THEN NULL
            ELSE SPLIT_PART(sbcustphone, '-', 1)
          END
        ) - LENGTH(
          REPLACE(
            CASE
              WHEN -CASE
                WHEN LENGTH('-') = 0
                THEN 0
                ELSE CAST(CAST((
                  LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
                ) AS DOUBLE) / LENGTH('-') AS BIGINT)
              END > 1
              THEN NULL
              WHEN CASE
                WHEN LENGTH('-') = 0
                THEN 0
                ELSE CAST(CAST((
                  LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
                ) AS DOUBLE) / LENGTH('-') AS BIGINT)
              END < 1
              THEN NULL
              ELSE SPLIT_PART(sbcustphone, '-', 1)
            END,
            '5',
            ''
          )
        )
      ) AS DOUBLE) / LENGTH('5') AS BIGINT)
    END < CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) < 0
    THEN SPLIT_PART(
      CASE
        WHEN -CASE
          WHEN LENGTH('-') = 0
          THEN 0
          ELSE CAST(CAST((
            LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
          ) AS DOUBLE) / LENGTH('-') AS BIGINT)
        END > 1
        THEN NULL
        WHEN CASE
          WHEN LENGTH('-') = 0
          THEN 0
          ELSE CAST(CAST((
            LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
          ) AS DOUBLE) / LENGTH('-') AS BIGINT)
        END < 1
        THEN NULL
        ELSE SPLIT_PART(sbcustphone, '-', 1)
      END,
      '5',
      CASE
        WHEN LENGTH('5') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(
            CASE
              WHEN -CASE
                WHEN LENGTH('-') = 0
                THEN 0
                ELSE CAST(CAST((
                  LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
                ) AS DOUBLE) / LENGTH('-') AS BIGINT)
              END > 1
              THEN NULL
              WHEN CASE
                WHEN LENGTH('-') = 0
                THEN 0
                ELSE CAST(CAST((
                  LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
                ) AS DOUBLE) / LENGTH('-') AS BIGINT)
              END < 1
              THEN NULL
              ELSE SPLIT_PART(sbcustphone, '-', 1)
            END
          ) - LENGTH(
            REPLACE(
              CASE
                WHEN -CASE
                  WHEN LENGTH('-') = 0
                  THEN 0
                  ELSE CAST(CAST((
                    LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
                  ) AS DOUBLE) / LENGTH('-') AS BIGINT)
                END > 1
                THEN NULL
                WHEN CASE
                  WHEN LENGTH('-') = 0
                  THEN 0
                  ELSE CAST(CAST((
                    LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
                  ) AS DOUBLE) / LENGTH('-') AS BIGINT)
                END < 1
                THEN NULL
                ELSE SPLIT_PART(sbcustphone, '-', 1)
              END,
              '5',
              ''
            )
          )
        ) AS DOUBLE) / LENGTH('5') AS BIGINT)
      END - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) + 1
    )
    ELSE SPLIT_PART(
      CASE
        WHEN -CASE
          WHEN LENGTH('-') = 0
          THEN 0
          ELSE CAST(CAST((
            LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
          ) AS DOUBLE) / LENGTH('-') AS BIGINT)
        END > 1
        THEN NULL
        WHEN CASE
          WHEN LENGTH('-') = 0
          THEN 0
          ELSE CAST(CAST((
            LENGTH(sbcustphone) - LENGTH(REPLACE(sbcustphone, '-', ''))
          ) AS DOUBLE) / LENGTH('-') AS BIGINT)
        END < 1
        THEN NULL
        ELSE SPLIT_PART(sbcustphone, '-', 1)
      END,
      '5',
      CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    )
  END AS p17,
  CASE
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) = 0
    THEN SPLIT_PART(sbcustpostalcode, '0', 1)
    WHEN -CASE
      WHEN LENGTH('0') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '0', ''))
      ) AS DOUBLE) / LENGTH('0') AS BIGINT)
    END > CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CASE
      WHEN LENGTH('0') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '0', ''))
      ) AS DOUBLE) / LENGTH('0') AS BIGINT)
    END < CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
    THEN NULL
    WHEN CAST(SUBSTRING(sbcustid, 2) AS BIGINT) < 0
    THEN SPLIT_PART(
      sbcustpostalcode,
      '0',
      CASE
        WHEN LENGTH('0') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(sbcustpostalcode) - LENGTH(REPLACE(sbcustpostalcode, '0', ''))
        ) AS DOUBLE) / LENGTH('0') AS BIGINT)
      END - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) + 1
    )
    ELSE SPLIT_PART(sbcustpostalcode, '0', CAST(SUBSTRING(sbcustid, 2) AS BIGINT))
  END AS p18
FROM main.sbcustomer
WHERE
  CAST(SUBSTRING(sbcustid, 2) AS BIGINT) <= 4
ORDER BY
  1 NULLS FIRST
