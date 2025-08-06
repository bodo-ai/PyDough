WITH _S2 AS (
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
), _S0 AS (
  SELECT
    0 AS PART_INDEX,
    '' AS PART,
    P_NAME AS REST,
    ' ' AS DELIM,
    -1 AS IDX
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
    LEAST(
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
      ),
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
      )
    )
  ) AS b,
  TRIM(SUBSTRING(p_name, 1, 2), 'o') AS c,
  LPAD(CAST(p_size AS TEXT), 3, '0') AS d,
  RPAD(CAST(p_size AS TEXT), 3, '0') AS e,
  REPLACE(p_mfgr, 'Manufacturer#', 'm') AS f,
  REPLACE(LOWER(p_container), ' ', '') AS g,
  CASE
    WHEN LENGTH('o') = 0
    THEN 0
    ELSE CAST(LENGTH(p_name) - LENGTH(REPLACE(p_name, 'o', '')) / LENGTH('o') AS BIGINT)
  END + (
    (
      CHARINDEX('o', p_name) - 1
    ) / 100.0
  ) AS h,
  ROUND(POWER(GREATEST(p_size, 10), 0.5), 3) AS i
FROM TPCH.PART
ORDER BY
  p_partkey NULLS FIRST
LIMIT 5
