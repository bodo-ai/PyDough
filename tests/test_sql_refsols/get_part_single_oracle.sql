WITH _S0 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    SBCUSTNAME AS REST,
    ' ' AS DELIM,
    -1 AS IDX
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
)
SELECT
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
  ) AS last_name
FROM MAIN.SBCUSTOMER
WHERE
  sbcustname = 'Alex Rodriguez'
