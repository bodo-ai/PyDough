WITH _S0 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    P_NAME AS REST,
    ' ' AS DELIM,
    1 AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN INSTR(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTR(rest, 1, INSTR(rest, delim) - 1)
    END AS PART,
    CASE
      WHEN INSTR(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTR(rest, INSTR(rest, delim) + LENGTH(delim))
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
    P_NAME AS REST,
    ' ' AS DELIM,
    2 AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN INSTR(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTR(rest, 1, INSTR(rest, delim) - 1)
    END AS PART,
    CASE
      WHEN INSTR(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTR(rest, INSTR(rest, delim) + LENGTH(delim))
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
    P_NAME AS REST,
    ' ' AS DELIM,
    3 AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN INSTR(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTR(rest, 1, INSTR(rest, delim) - 1)
    END AS PART,
    CASE
      WHEN INSTR(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTR(rest, INSTR(rest, delim) + LENGTH(delim))
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
    P_NAME AS REST,
    ' ' AS DELIM,
    4 AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN INSTR(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTR(rest, 1, INSTR(rest, delim) - 1)
    END AS PART,
    CASE
      WHEN INSTR(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTR(rest, INSTR(rest, delim) + LENGTH(delim))
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
    P_NAME AS REST,
    ' ' AS DELIM,
    5 AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN INSTR(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTR(rest, 1, INSTR(rest, delim) - 1)
    END AS PART,
    CASE
      WHEN INSTR(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTR(rest, INSTR(rest, delim) + LENGTH(delim))
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
    P_NAME AS REST,
    ' ' AS DELIM,
    6 AS IDX
  UNION ALL
  SELECT
    part_index + 1 AS PART_INDEX,
    CASE
      WHEN INSTR(rest, delim) = 0 OR delim = ''
      THEN rest
      ELSE SUBSTR(rest, 1, INSTR(rest, delim) - 1)
    END AS PART,
    CASE
      WHEN INSTR(rest, delim) = 0 OR delim = ''
      THEN ''
      ELSE SUBSTR(rest, INSTR(rest, delim) + LENGTH(delim))
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
)
SELECT
  p_partkey AS key,
  UPPER(
    (
      SELECT
        _S0.PART
      FROM _S0 _S0
      CROSS JOIN _S1 _S1
      WHERE
        _S0.PART_INDEX <> 0
        AND _S0.PART_INDEX = CASE
          WHEN _S0.IDX > 0
          THEN _S0.IDX
          WHEN _S0.IDX < 0
          THEN _S1.TOTAL_PARTS + _S0.IDX + 1
          ELSE 1
        END
    )
  ) AS c1,
  UPPER(
    (
      SELECT
        _S2.PART
      FROM _S2 _S2
      CROSS JOIN _S3 _S3
      WHERE
        _S2.PART_INDEX <> 0
        AND _S2.PART_INDEX = CASE
          WHEN _S2.IDX > 0
          THEN _S2.IDX
          WHEN _S2.IDX < 0
          THEN _S3.TOTAL_PARTS + _S2.IDX + 1
          ELSE 1
        END
    )
  ) AS c2,
  UPPER(
    (
      SELECT
        _S4.PART
      FROM _S4 _S4
      CROSS JOIN _S5 _S5
      WHERE
        _S4.PART_INDEX <> 0
        AND _S4.PART_INDEX = CASE
          WHEN _S4.IDX > 0
          THEN _S4.IDX
          WHEN _S4.IDX < 0
          THEN _S5.TOTAL_PARTS + _S4.IDX + 1
          ELSE 1
        END
    )
  ) AS c3,
  UPPER(
    (
      SELECT
        _S6.PART
      FROM _S6 _S6
      CROSS JOIN _S7 _S7
      WHERE
        _S6.PART_INDEX <> 0
        AND _S6.PART_INDEX = CASE
          WHEN _S6.IDX > 0
          THEN _S6.IDX
          WHEN _S6.IDX < 0
          THEN _S7.TOTAL_PARTS + _S6.IDX + 1
          ELSE 1
        END
    )
  ) AS c4,
  UPPER(
    (
      SELECT
        _S8.PART
      FROM _S8 _S8
      CROSS JOIN _S9 _S9
      WHERE
        _S8.PART_INDEX <> 0
        AND _S8.PART_INDEX = CASE
          WHEN _S8.IDX > 0
          THEN _S8.IDX
          WHEN _S8.IDX < 0
          THEN _S9.TOTAL_PARTS + _S8.IDX + 1
          ELSE 1
        END
    )
  ) AS c5,
  UPPER(
    (
      SELECT
        _S10.PART
      FROM _S10 _S10
      CROSS JOIN _S11 _S11
      WHERE
        _S10.PART_INDEX <> 0
        AND _S10.PART_INDEX = CASE
          WHEN _S10.IDX > 0
          THEN _S10.IDX
          WHEN _S10.IDX < 0
          THEN _S11.TOTAL_PARTS + _S10.IDX + 1
          ELSE 1
        END
    )
  ) AS c6
FROM TPCH.PART
ORDER BY
  1 NULLS FIRST
FETCH FIRST 5 ROWS ONLY
