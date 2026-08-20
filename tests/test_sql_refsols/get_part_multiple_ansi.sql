WITH _s0 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustname AS rest,
    ' ' AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s0
  WHERE
    is_last = 0
), _s1 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s0
), _s2 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustname AS rest,
    ' ' AS delim,
    -1 * CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s2
  WHERE
    is_last = 0
), _s3 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s2
), _s4 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustemail AS rest,
    '.' AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s4
  WHERE
    is_last = 0
), _s5 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s4
), _s6 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustemail AS rest,
    '.' AS delim,
    -1 * CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s6
  WHERE
    is_last = 0
), _s7 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s6
), _s8 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustphone AS rest,
    '-' AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s8
  WHERE
    is_last = 0
), _s9 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s8
), _s10 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustphone AS rest,
    '-' AS delim,
    -1 * CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s10
  WHERE
    is_last = 0
), _s11 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s10
), _s12 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustpostalcode AS rest,
    '00' AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s12
  WHERE
    is_last = 0
), _s13 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s12
), _s14 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustpostalcode AS rest,
    '00' AS delim,
    -1 * CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s14
  WHERE
    is_last = 0
), _s15 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s14
), _s16 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustname AS rest,
    '!' AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s16
  WHERE
    is_last = 0
), _s17 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s16
), _s18 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustname AS rest,
    '@' AS delim,
    -1 * CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s18
  WHERE
    is_last = 0
), _s19 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s18
), _s20 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustname AS rest,
    'aa' AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s20
  WHERE
    is_last = 0
), _s21 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s20
), _s22 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustname AS rest,
    '#$*' AS delim,
    -1 * CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s22
  WHERE
    is_last = 0
), _s23 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s22
), _s24 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustname AS rest,
    '' AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s24
  WHERE
    is_last = 0
), _s25 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s24
), _s26 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    '' AS rest,
    ' ' AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s26
  WHERE
    is_last = 0
), _s27 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s26
), _s28 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustname AS rest,
    ' ' AS delim,
    0 AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s28
  WHERE
    is_last = 0
), _s29 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s28
), _s30 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcuststate AS rest,
    sbcuststate AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s30
  WHERE
    is_last = 0
), _s31 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s30
), _s32 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustphone AS rest,
    '-' AS delim,
    1 AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s32
  WHERE
    is_last = 0
), _s33 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s32
), _s34 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    COALESCE(
      (
        SELECT
          _s32.part
        FROM _s32 AS _s32
        CROSS JOIN _s33 AS _s33
        WHERE
          _s32.part_index <> 0
          AND _s32.part_index = CASE
            WHEN _s32.idx > 0
            THEN _s32.idx
            WHEN _s32.idx < 0
            THEN _s33.total_parts + _s32.idx + 1
            ELSE 1
          END
      ),
      ''
    ) AS rest,
    '5' AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s34
  WHERE
    is_last = 0
), _s35 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s34
), _s36 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustpostalcode AS rest,
    '0' AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx,
    0 AS is_last
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, STR_POSITION(rest, delim) - 1)
    END AS part,
    CASE
      WHEN STR_POSITION(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, STR_POSITION(rest, delim) + LENGTH(delim))
    END AS rest,
    delim,
    idx,
    CASE WHEN STR_POSITION(rest, delim) = 0 OR delim = '' THEN 1 ELSE 0 END AS is_last
  FROM _s36
  WHERE
    is_last = 0
), _s37 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s36
)
SELECT
  CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS _expr0,
  COALESCE(
    (
      SELECT
        _s0.part
      FROM _s0 AS _s0
      CROSS JOIN _s1 AS _s1
      WHERE
        _s0.part_index <> 0
        AND _s0.part_index = CASE
          WHEN _s0.idx > 0
          THEN _s0.idx
          WHEN _s0.idx < 0
          THEN _s1.total_parts + _s0.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p1,
  COALESCE(
    (
      SELECT
        _s2.part
      FROM _s2 AS _s2
      CROSS JOIN _s3 AS _s3
      WHERE
        _s2.part_index <> 0
        AND _s2.part_index = CASE
          WHEN _s2.idx > 0
          THEN _s2.idx
          WHEN _s2.idx < 0
          THEN _s3.total_parts + _s2.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p2,
  COALESCE(
    (
      SELECT
        _s4.part
      FROM _s4 AS _s4
      CROSS JOIN _s5 AS _s5
      WHERE
        _s4.part_index <> 0
        AND _s4.part_index = CASE
          WHEN _s4.idx > 0
          THEN _s4.idx
          WHEN _s4.idx < 0
          THEN _s5.total_parts + _s4.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p3,
  COALESCE(
    (
      SELECT
        _s6.part
      FROM _s6 AS _s6
      CROSS JOIN _s7 AS _s7
      WHERE
        _s6.part_index <> 0
        AND _s6.part_index = CASE
          WHEN _s6.idx > 0
          THEN _s6.idx
          WHEN _s6.idx < 0
          THEN _s7.total_parts + _s6.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p4,
  COALESCE(
    (
      SELECT
        _s8.part
      FROM _s8 AS _s8
      CROSS JOIN _s9 AS _s9
      WHERE
        _s8.part_index <> 0
        AND _s8.part_index = CASE
          WHEN _s8.idx > 0
          THEN _s8.idx
          WHEN _s8.idx < 0
          THEN _s9.total_parts + _s8.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p5,
  COALESCE(
    (
      SELECT
        _s10.part
      FROM _s10 AS _s10
      CROSS JOIN _s11 AS _s11
      WHERE
        _s10.part_index <> 0
        AND _s10.part_index = CASE
          WHEN _s10.idx > 0
          THEN _s10.idx
          WHEN _s10.idx < 0
          THEN _s11.total_parts + _s10.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p6,
  COALESCE(
    (
      SELECT
        _s12.part
      FROM _s12 AS _s12
      CROSS JOIN _s13 AS _s13
      WHERE
        _s12.part_index <> 0
        AND _s12.part_index = CASE
          WHEN _s12.idx > 0
          THEN _s12.idx
          WHEN _s12.idx < 0
          THEN _s13.total_parts + _s12.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p7,
  COALESCE(
    (
      SELECT
        _s14.part
      FROM _s14 AS _s14
      CROSS JOIN _s15 AS _s15
      WHERE
        _s14.part_index <> 0
        AND _s14.part_index = CASE
          WHEN _s14.idx > 0
          THEN _s14.idx
          WHEN _s14.idx < 0
          THEN _s15.total_parts + _s14.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p8,
  COALESCE(
    (
      SELECT
        _s16.part
      FROM _s16 AS _s16
      CROSS JOIN _s17 AS _s17
      WHERE
        _s16.part_index <> 0
        AND _s16.part_index = CASE
          WHEN _s16.idx > 0
          THEN _s16.idx
          WHEN _s16.idx < 0
          THEN _s17.total_parts + _s16.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p9,
  COALESCE(
    (
      SELECT
        _s18.part
      FROM _s18 AS _s18
      CROSS JOIN _s19 AS _s19
      WHERE
        _s18.part_index <> 0
        AND _s18.part_index = CASE
          WHEN _s18.idx > 0
          THEN _s18.idx
          WHEN _s18.idx < 0
          THEN _s19.total_parts + _s18.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p10,
  COALESCE(
    (
      SELECT
        _s20.part
      FROM _s20 AS _s20
      CROSS JOIN _s21 AS _s21
      WHERE
        _s20.part_index <> 0
        AND _s20.part_index = CASE
          WHEN _s20.idx > 0
          THEN _s20.idx
          WHEN _s20.idx < 0
          THEN _s21.total_parts + _s20.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p11,
  COALESCE(
    (
      SELECT
        _s22.part
      FROM _s22 AS _s22
      CROSS JOIN _s23 AS _s23
      WHERE
        _s22.part_index <> 0
        AND _s22.part_index = CASE
          WHEN _s22.idx > 0
          THEN _s22.idx
          WHEN _s22.idx < 0
          THEN _s23.total_parts + _s22.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p12,
  COALESCE(
    (
      SELECT
        _s24.part
      FROM _s24 AS _s24
      CROSS JOIN _s25 AS _s25
      WHERE
        _s24.part_index <> 0
        AND _s24.part_index = CASE
          WHEN _s24.idx > 0
          THEN _s24.idx
          WHEN _s24.idx < 0
          THEN _s25.total_parts + _s24.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p13,
  COALESCE(
    (
      SELECT
        _s26.part
      FROM _s26 AS _s26
      CROSS JOIN _s27 AS _s27
      WHERE
        _s26.part_index <> 0
        AND _s26.part_index = CASE
          WHEN _s26.idx > 0
          THEN _s26.idx
          WHEN _s26.idx < 0
          THEN _s27.total_parts + _s26.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p14,
  COALESCE(
    (
      SELECT
        _s28.part
      FROM _s28 AS _s28
      CROSS JOIN _s29 AS _s29
      WHERE
        _s28.part_index <> 0
        AND _s28.part_index = CASE
          WHEN _s28.idx > 0
          THEN _s28.idx
          WHEN _s28.idx < 0
          THEN _s29.total_parts + _s28.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p15,
  COALESCE(
    (
      SELECT
        _s30.part
      FROM _s30 AS _s30
      CROSS JOIN _s31 AS _s31
      WHERE
        _s30.part_index <> 0
        AND _s30.part_index = CASE
          WHEN _s30.idx > 0
          THEN _s30.idx
          WHEN _s30.idx < 0
          THEN _s31.total_parts + _s30.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p16,
  COALESCE(
    (
      SELECT
        _s34.part
      FROM _s34 AS _s34
      CROSS JOIN _s35 AS _s35
      WHERE
        _s34.part_index <> 0
        AND _s34.part_index = CASE
          WHEN _s34.idx > 0
          THEN _s34.idx
          WHEN _s34.idx < 0
          THEN _s35.total_parts + _s34.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p17,
  COALESCE(
    (
      SELECT
        _s36.part
      FROM _s36 AS _s36
      CROSS JOIN _s37 AS _s37
      WHERE
        _s36.part_index <> 0
        AND _s36.part_index = CASE
          WHEN _s36.idx > 0
          THEN _s36.idx
          WHEN _s36.idx < 0
          THEN _s37.total_parts + _s36.idx + 1
          ELSE 1
        END
    ),
    ''
  ) AS p18
FROM main.sbcustomer
WHERE
  CAST(SUBSTRING(sbcustid, 2) AS BIGINT) <= 4
ORDER BY
  1
