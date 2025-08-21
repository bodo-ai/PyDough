WITH _s2 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    p_name AS rest,
    ' ' AS delim,
    2 AS idx
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
), _s0 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    p_name AS rest,
    ' ' AS delim,
    -1 AS idx
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
)
SELECT
  p_partkey AS key,
  CAST(CONCAT_WS(
    '',
    SUBSTRING(
      p_brand,
      CASE WHEN (
        LENGTH(p_brand) + -1
      ) < 1 THEN 1 ELSE (
        LENGTH(p_brand) + -1
      ) END
    ),
    SUBSTRING(p_brand, 8),
    SUBSTRING(
      p_brand,
      CASE WHEN (
        LENGTH(p_brand) + -1
      ) < 1 THEN 1 ELSE (
        LENGTH(p_brand) + -1
      ) END,
      CASE
        WHEN (
          LENGTH(p_brand) + 0
        ) < 1
        THEN 0
        ELSE (
          LENGTH(p_brand) + 0
        ) - CASE WHEN (
          LENGTH(p_brand) + -1
        ) < 1 THEN 1 ELSE (
          LENGTH(p_brand) + -1
        ) END
      END
    )
  ) AS BIGINT) AS a,
  UPPER(
    CASE
      WHEN (
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
      ) >= (
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
      )
      THEN (
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
      )
      WHEN (
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
      ) <= (
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
      )
      THEN (
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
      )
    END
  ) AS b,
  TRIM(SUBSTRING(p_name, 1, 2), 'o') AS c,
  CASE
    WHEN LENGTH(CAST(p_size AS TEXT)) >= 3
    THEN SUBSTRING(CAST(p_size AS TEXT), 1, 3)
    ELSE SUBSTRING(CONCAT('000', CAST(p_size AS TEXT)), -3)
  END AS d,
  SUBSTRING(CONCAT(CAST(p_size AS TEXT), '000'), 1, 3) AS e,
  REPLACE(p_mfgr, 'Manufacturer#', 'm') AS f,
  REPLACE(LOWER(p_container), ' ', '') AS g,
  CASE
    WHEN LENGTH('o') = 0
    THEN 0
    ELSE CAST((
      LENGTH(p_name) - LENGTH(REPLACE(p_name, 'o', ''))
    ) / LENGTH('o') AS BIGINT)
  END + (
    (
      STR_POSITION(p_name, 'o') - 1
    ) / 100.0
  ) AS h,
  ROUND(POWER(CASE WHEN p_size >= 10 THEN p_size WHEN p_size <= 10 THEN 10 END, 0.5), 3) AS i
FROM tpch.part
ORDER BY
  1
LIMIT 5
