WITH _s0 AS (
  SELECT
    0 AS part_index,
    '' AS part,
    sbcustname AS rest,
    ' ' AS delim,
    -1 AS idx,
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
)
SELECT
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
  ) AS last_name
FROM main.sbcustomer
WHERE
  sbcustname = 'Alex Rodriguez'
