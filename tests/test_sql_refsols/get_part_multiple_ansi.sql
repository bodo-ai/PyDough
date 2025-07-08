WITH _s0 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustname AS rest,
    ' ' AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx
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
    idx
  FROM _s0
  WHERE
    rest <> ''
), _s1 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s0
), _s10 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustname AS rest,
    ' ' AS delim,
    0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx
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
    idx
  FROM _s10
  WHERE
    rest <> ''
), _s11 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s10
), _s12 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustemail AS rest,
    '.' AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx
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
    idx
  FROM _s12
  WHERE
    rest <> ''
), _s13 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s12
), _s14 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustemail AS rest,
    '.' AS delim,
    0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx
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
    idx
  FROM _s14
  WHERE
    rest <> ''
), _s15 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s14
), _s16 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustphone AS rest,
    '-' AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx
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
    idx
  FROM _s16
  WHERE
    rest <> ''
), _s17 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s16
), _s18 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustphone AS rest,
    '-' AS delim,
    0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx
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
    idx
  FROM _s18
  WHERE
    rest <> ''
), _s19 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s18
), _s2 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustname AS rest,
    '@' AS delim,
    0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx
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
    idx
  FROM _s2
  WHERE
    rest <> ''
), _s3 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s2
), _s20 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustpostalcode AS rest,
    '00' AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx
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
    idx
  FROM _s20
  WHERE
    rest <> ''
), _s21 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s20
), _s22 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustpostalcode AS rest,
    '00' AS delim,
    0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx
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
    idx
  FROM _s22
  WHERE
    rest <> ''
), _s23 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s22
), _s24 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustname AS rest,
    '!' AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx
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
    idx
  FROM _s24
  WHERE
    rest <> ''
), _s25 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s24
), _s4 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustname AS rest,
    'aa' AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx
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
    idx
  FROM _s4
  WHERE
    rest <> ''
), _s5 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s4
), _s6 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustname AS rest,
    '#$*' AS delim,
    0 - CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx
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
    idx
  FROM _s6
  WHERE
    rest <> ''
), _s7 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s6
), _s8 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustname AS rest,
    '' AS delim,
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS idx
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
    idx
  FROM _s8
  WHERE
    rest <> ''
), _s9 AS (
  SELECT
    COUNT(*) - 1 AS total_parts
  FROM _s8
), _t1 AS (
  SELECT
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS _expr0,
    sbcustid,
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
    ) AS p1,
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
    ) AS p2,
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
    ) AS p3,
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
    ) AS p4,
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
    ) AS p5,
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
    ) AS p6,
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
    ) AS p10,
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
    ) AS p7,
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
    ) AS p8,
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
    ) AS p9,
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
    ) AS p11,
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
    ) AS p12,
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
    ) AS p13
  FROM main.sbcustomer
  WHERE
    CAST(SUBSTRING(sbcustid, 2) AS BIGINT) <= 4
)
SELECT
  _expr0,
  p1,
  p2,
  p3,
  p4,
  p5,
  p6,
  p7,
  p8,
  p9,
  p10,
  p11,
  p12,
  p13
FROM _t1
ORDER BY
  CAST(SUBSTRING(sbcustid, 2) AS BIGINT)
