WITH _s0 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    p_name AS rest,
    ' ' AS delim,
    1 AS idx
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN INSTR(rest, delim) = 0
      THEN rest
      ELSE SUBSTRING(rest, 1, INSTR(rest, delim) - 1)
    END AS part,
    CASE
      WHEN INSTR(rest, delim) = 0
      THEN ''
      ELSE SUBSTRING(rest, INSTR(rest, delim) + LENGTH(delim))
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
), _s2 AS (
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
      WHEN INSTR(rest, delim) = 0
      THEN rest
      ELSE SUBSTRING(rest, 1, INSTR(rest, delim) - 1)
    END AS part,
    CASE
      WHEN INSTR(rest, delim) = 0
      THEN ''
      ELSE SUBSTRING(rest, INSTR(rest, delim) + LENGTH(delim))
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
), _s4 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    p_name AS rest,
    ' ' AS delim,
    3 AS idx
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN INSTR(rest, delim) = 0
      THEN rest
      ELSE SUBSTRING(rest, 1, INSTR(rest, delim) - 1)
    END AS part,
    CASE
      WHEN INSTR(rest, delim) = 0
      THEN ''
      ELSE SUBSTRING(rest, INSTR(rest, delim) + LENGTH(delim))
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
    p_name AS rest,
    ' ' AS delim,
    4 AS idx
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN INSTR(rest, delim) = 0
      THEN rest
      ELSE SUBSTRING(rest, 1, INSTR(rest, delim) - 1)
    END AS part,
    CASE
      WHEN INSTR(rest, delim) = 0
      THEN ''
      ELSE SUBSTRING(rest, INSTR(rest, delim) + LENGTH(delim))
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
    p_name AS rest,
    ' ' AS delim,
    5 AS idx
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN INSTR(rest, delim) = 0
      THEN rest
      ELSE SUBSTRING(rest, 1, INSTR(rest, delim) - 1)
    END AS part,
    CASE
      WHEN INSTR(rest, delim) = 0
      THEN ''
      ELSE SUBSTRING(rest, INSTR(rest, delim) + LENGTH(delim))
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
), _s10 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    p_name AS rest,
    ' ' AS delim,
    6 AS idx
  UNION ALL
  SELECT
    part_index + 1 AS part_index,
    CASE
      WHEN INSTR(rest, delim) = 0
      THEN rest
      ELSE SUBSTRING(rest, 1, INSTR(rest, delim) - 1)
    END AS part,
    CASE
      WHEN INSTR(rest, delim) = 0
      THEN ''
      ELSE SUBSTRING(rest, INSTR(rest, delim) + LENGTH(delim))
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
)
SELECT
  p_partkey AS key,
  UPPER(
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
    )
  ) AS c1,
  UPPER(
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
    )
  ) AS c2,
  UPPER(
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
    )
  ) AS c3,
  UPPER(
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
    )
  ) AS c4,
  UPPER(
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
    )
  ) AS c5,
  UPPER(
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
    )
  ) AS c6
FROM tpch.part
ORDER BY
  p_partkey
LIMIT 5
