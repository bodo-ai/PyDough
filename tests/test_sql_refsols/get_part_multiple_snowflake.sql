WITH _S0 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTNAME AS REST,
    ' ' AS DELIM,
    CAST(SUBSTRING(SBCUSTID, 2) AS BIGINT) AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S0
  WHERE
    rest <> ''
), _S1 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S0
), _S2 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTNAME AS REST,
    ' ' AS DELIM,
    0 - CAST(SUBSTRING(SBCUSTID, 2) AS BIGINT) AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S2
  WHERE
    rest <> ''
), _S3 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S2
), _S4 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTEMAIL AS REST,
    '.' AS DELIM,
    CAST(SUBSTRING(SBCUSTID, 2) AS BIGINT) AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S4
  WHERE
    rest <> ''
), _S5 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S4
), _S6 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTEMAIL AS REST,
    '.' AS DELIM,
    0 - CAST(SUBSTRING(SBCUSTID, 2) AS BIGINT) AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S6
  WHERE
    rest <> ''
), _S7 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S6
), _S8 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTPHONE AS REST,
    '-' AS DELIM,
    CAST(SUBSTRING(SBCUSTID, 2) AS BIGINT) AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S8
  WHERE
    rest <> ''
), _S9 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S8
), _S10 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTPHONE AS REST,
    '-' AS DELIM,
    0 - CAST(SUBSTRING(SBCUSTID, 2) AS BIGINT) AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S10
  WHERE
    rest <> ''
), _S11 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S10
), _S12 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTPOSTALCODE AS REST,
    '00' AS DELIM,
    CAST(SUBSTRING(SBCUSTID, 2) AS BIGINT) AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S12
  WHERE
    rest <> ''
), _S13 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S12
), _S14 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTPOSTALCODE AS REST,
    '00' AS DELIM,
    0 - CAST(SUBSTRING(SBCUSTID, 2) AS BIGINT) AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S14
  WHERE
    rest <> ''
), _S15 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S14
), _S16 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTNAME AS REST,
    '!' AS DELIM,
    CAST(SUBSTRING(SBCUSTID, 2) AS BIGINT) AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S16
  WHERE
    rest <> ''
), _S17 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S16
), _S18 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTNAME AS REST,
    '@' AS DELIM,
    0 - CAST(SUBSTRING(SBCUSTID, 2) AS BIGINT) AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S18
  WHERE
    rest <> ''
), _S19 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S18
), _S20 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTNAME AS REST,
    'aa' AS DELIM,
    CAST(SUBSTRING(SBCUSTID, 2) AS BIGINT) AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S20
  WHERE
    rest <> ''
), _S21 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S20
), _S22 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTNAME AS REST,
    '#$*' AS DELIM,
    0 - CAST(SUBSTRING(SBCUSTID, 2) AS BIGINT) AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S22
  WHERE
    rest <> ''
), _S23 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S22
), _S24 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTNAME AS REST,
    '' AS DELIM,
    CAST(SUBSTRING(SBCUSTID, 2) AS BIGINT) AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S24
  WHERE
    rest <> ''
), _S25 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S24
), _S26 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    '' AS REST,
    ' ' AS DELIM,
    CAST(SUBSTRING(SBCUSTID, 2) AS BIGINT) AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S26
  WHERE
    rest <> ''
), _S27 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S26
), _S28 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTNAME AS REST,
    ' ' AS DELIM,
    0 AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S28
  WHERE
    rest <> ''
), _S29 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S28
), _S30 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTSTATE AS REST,
    SBCUSTSTATE AS DELIM,
    CAST(SUBSTRING(SBCUSTID, 2) AS BIGINT) AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S30
  WHERE
    rest <> ''
), _S31 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S30
), _S32 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTPHONE AS REST,
    '-' AS DELIM,
    1 AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S32
  WHERE
    rest <> ''
), _S33 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S32
), _S34 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    (
      SELECT
        _S32.PART
      FROM _S32 AS _S32
      CROSS JOIN _S33 AS _S33
      WHERE
        _S32.PART_INDEX <> 0
        AND _S32.PART_INDEX = CASE
          WHEN _S32.IDX > 0
          THEN _S32.IDX
          WHEN _S32.IDX < 0
          THEN _S33.TOTAL_PARTS + _S32.IDX + 1
          ELSE 1
        END
    ) AS REST,
    '5' AS DELIM,
    CAST(SUBSTRING(SBCUSTID, 2) AS BIGINT) AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S34
  WHERE
    rest <> ''
), _S35 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S34
), _S36 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTPOSTALCODE AS REST,
    '0' AS DELIM,
    CAST(SUBSTRING(SBCUSTID, 2) AS BIGINT) AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTRING(rest, 1, CHARINDEX(delim, rest) - 1)
    END AS PART,
    CASE
      WHEN CHARINDEX(delim, rest) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTRING(rest, CHARINDEX(delim, rest) + LENGTH(delim))
    END AS REST,
    delim AS DELIM,
    idx AS IDX
  FROM _S36
  WHERE
    rest <> ''
), _S37 AS (
  SELECT
    COUNT(*) - 1 AS TOTAL_PARTS
  FROM _S36
)
SELECT
  CAST(SUBSTRING(sbcustid, 2) AS BIGINT) AS _expr0,
  (
    SELECT
      _S0.PART
    FROM _S0 AS _S0
    CROSS JOIN _S1 AS _S1
    WHERE
      _S0.PART_INDEX <> 0
      AND _S0.PART_INDEX = CASE
        WHEN _S0.IDX > 0
        THEN _S0.IDX
        WHEN _S0.IDX < 0
        THEN _S1.TOTAL_PARTS + _S0.IDX + 1
        ELSE 1
      END
  ) AS p1,
  (
    SELECT
      _S2.PART
    FROM _S2 AS _S2
    CROSS JOIN _S3 AS _S3
    WHERE
      _S2.PART_INDEX <> 0
      AND _S2.PART_INDEX = CASE
        WHEN _S2.IDX > 0
        THEN _S2.IDX
        WHEN _S2.IDX < 0
        THEN _S3.TOTAL_PARTS + _S2.IDX + 1
        ELSE 1
      END
  ) AS p2,
  (
    SELECT
      _S4.PART
    FROM _S4 AS _S4
    CROSS JOIN _S5 AS _S5
    WHERE
      _S4.PART_INDEX <> 0
      AND _S4.PART_INDEX = CASE
        WHEN _S4.IDX > 0
        THEN _S4.IDX
        WHEN _S4.IDX < 0
        THEN _S5.TOTAL_PARTS + _S4.IDX + 1
        ELSE 1
      END
  ) AS p3,
  (
    SELECT
      _S6.PART
    FROM _S6 AS _S6
    CROSS JOIN _S7 AS _S7
    WHERE
      _S6.PART_INDEX <> 0
      AND _S6.PART_INDEX = CASE
        WHEN _S6.IDX > 0
        THEN _S6.IDX
        WHEN _S6.IDX < 0
        THEN _S7.TOTAL_PARTS + _S6.IDX + 1
        ELSE 1
      END
  ) AS p4,
  (
    SELECT
      _S8.PART
    FROM _S8 AS _S8
    CROSS JOIN _S9 AS _S9
    WHERE
      _S8.PART_INDEX <> 0
      AND _S8.PART_INDEX = CASE
        WHEN _S8.IDX > 0
        THEN _S8.IDX
        WHEN _S8.IDX < 0
        THEN _S9.TOTAL_PARTS + _S8.IDX + 1
        ELSE 1
      END
  ) AS p5,
  (
    SELECT
      _S10.PART
    FROM _S10 AS _S10
    CROSS JOIN _S11 AS _S11
    WHERE
      _S10.PART_INDEX <> 0
      AND _S10.PART_INDEX = CASE
        WHEN _S10.IDX > 0
        THEN _S10.IDX
        WHEN _S10.IDX < 0
        THEN _S11.TOTAL_PARTS + _S10.IDX + 1
        ELSE 1
      END
  ) AS p6,
  (
    SELECT
      _S12.PART
    FROM _S12 AS _S12
    CROSS JOIN _S13 AS _S13
    WHERE
      _S12.PART_INDEX <> 0
      AND _S12.PART_INDEX = CASE
        WHEN _S12.IDX > 0
        THEN _S12.IDX
        WHEN _S12.IDX < 0
        THEN _S13.TOTAL_PARTS + _S12.IDX + 1
        ELSE 1
      END
  ) AS p7,
  (
    SELECT
      _S14.PART
    FROM _S14 AS _S14
    CROSS JOIN _S15 AS _S15
    WHERE
      _S14.PART_INDEX <> 0
      AND _S14.PART_INDEX = CASE
        WHEN _S14.IDX > 0
        THEN _S14.IDX
        WHEN _S14.IDX < 0
        THEN _S15.TOTAL_PARTS + _S14.IDX + 1
        ELSE 1
      END
  ) AS p8,
  (
    SELECT
      _S16.PART
    FROM _S16 AS _S16
    CROSS JOIN _S17 AS _S17
    WHERE
      _S16.PART_INDEX <> 0
      AND _S16.PART_INDEX = CASE
        WHEN _S16.IDX > 0
        THEN _S16.IDX
        WHEN _S16.IDX < 0
        THEN _S17.TOTAL_PARTS + _S16.IDX + 1
        ELSE 1
      END
  ) AS p9,
  (
    SELECT
      _S18.PART
    FROM _S18 AS _S18
    CROSS JOIN _S19 AS _S19
    WHERE
      _S18.PART_INDEX <> 0
      AND _S18.PART_INDEX = CASE
        WHEN _S18.IDX > 0
        THEN _S18.IDX
        WHEN _S18.IDX < 0
        THEN _S19.TOTAL_PARTS + _S18.IDX + 1
        ELSE 1
      END
  ) AS p10,
  (
    SELECT
      _S20.PART
    FROM _S20 AS _S20
    CROSS JOIN _S21 AS _S21
    WHERE
      _S20.PART_INDEX <> 0
      AND _S20.PART_INDEX = CASE
        WHEN _S20.IDX > 0
        THEN _S20.IDX
        WHEN _S20.IDX < 0
        THEN _S21.TOTAL_PARTS + _S20.IDX + 1
        ELSE 1
      END
  ) AS p11,
  (
    SELECT
      _S22.PART
    FROM _S22 AS _S22
    CROSS JOIN _S23 AS _S23
    WHERE
      _S22.PART_INDEX <> 0
      AND _S22.PART_INDEX = CASE
        WHEN _S22.IDX > 0
        THEN _S22.IDX
        WHEN _S22.IDX < 0
        THEN _S23.TOTAL_PARTS + _S22.IDX + 1
        ELSE 1
      END
  ) AS p12,
  (
    SELECT
      _S24.PART
    FROM _S24 AS _S24
    CROSS JOIN _S25 AS _S25
    WHERE
      _S24.PART_INDEX <> 0
      AND _S24.PART_INDEX = CASE
        WHEN _S24.IDX > 0
        THEN _S24.IDX
        WHEN _S24.IDX < 0
        THEN _S25.TOTAL_PARTS + _S24.IDX + 1
        ELSE 1
      END
  ) AS p13,
  (
    SELECT
      _S26.PART
    FROM _S26 AS _S26
    CROSS JOIN _S27 AS _S27
    WHERE
      _S26.PART_INDEX <> 0
      AND _S26.PART_INDEX = CASE
        WHEN _S26.IDX > 0
        THEN _S26.IDX
        WHEN _S26.IDX < 0
        THEN _S27.TOTAL_PARTS + _S26.IDX + 1
        ELSE 1
      END
  ) AS p14,
  (
    SELECT
      _S28.PART
    FROM _S28 AS _S28
    CROSS JOIN _S29 AS _S29
    WHERE
      _S28.PART_INDEX <> 0
      AND _S28.PART_INDEX = CASE
        WHEN _S28.IDX > 0
        THEN _S28.IDX
        WHEN _S28.IDX < 0
        THEN _S29.TOTAL_PARTS + _S28.IDX + 1
        ELSE 1
      END
  ) AS p15,
  (
    SELECT
      _S30.PART
    FROM _S30 AS _S30
    CROSS JOIN _S31 AS _S31
    WHERE
      _S30.PART_INDEX <> 0
      AND _S30.PART_INDEX = CASE
        WHEN _S30.IDX > 0
        THEN _S30.IDX
        WHEN _S30.IDX < 0
        THEN _S31.TOTAL_PARTS + _S30.IDX + 1
        ELSE 1
      END
  ) AS p16,
  (
    SELECT
      _S34.PART
    FROM _S34 AS _S34
    CROSS JOIN _S35 AS _S35
    WHERE
      _S34.PART_INDEX <> 0
      AND _S34.PART_INDEX = CASE
        WHEN _S34.IDX > 0
        THEN _S34.IDX
        WHEN _S34.IDX < 0
        THEN _S35.TOTAL_PARTS + _S34.IDX + 1
        ELSE 1
      END
  ) AS p17,
  (
    SELECT
      _S36.PART
    FROM _S36 AS _S36
    CROSS JOIN _S37 AS _S37
    WHERE
      _S36.PART_INDEX <> 0
      AND _S36.PART_INDEX = CASE
        WHEN _S36.IDX > 0
        THEN _S36.IDX
        WHEN _S36.IDX < 0
        THEN _S37.TOTAL_PARTS + _S36.IDX + 1
        ELSE 1
      END
  ) AS p18
FROM MAIN.SBCUSTOMER
WHERE
  CAST(SUBSTRING(sbcustid, 2) AS BIGINT) <= 4
ORDER BY
  CAST(SUBSTRING(sbcustid, 2) AS BIGINT) NULLS FIRST
