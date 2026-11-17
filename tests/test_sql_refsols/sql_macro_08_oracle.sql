WITH "_0" AS (
  SELECT DISTINCT
    REGEXP_SUBSTR(P.P_NAME, '[^ ]+', 1, L.LVL) AS COLOR
  FROM PART P
  CROSS JOIN LATERAL (
    SELECT
      "LEVEL" AS LVL
    FROM DUAL
    CONNECT BY "LEVEL" <= REGEXP_COUNT(P.P_NAME, '[^ ]+')
  ) AS L
), "_s0" AS (
  SELECT
    COLOR
  FROM "_0"
  ORDER BY
    1 NULLS FIRST
  FETCH FIRST 5 ROWS ONLY
)
SELECT
  "_s0".COLOR AS color,
  COUNT(*) AS n_parts
FROM "_s0" "_s0"
JOIN TPCH.PART PART
  ON PART.P_NAME LIKE CONCAT('%', (
    ' ' || NVL("_s0".COLOR, '')
  ))
  OR PART.P_NAME LIKE CONCAT('%', (
    ' ' || NVL("_s0".COLOR, '') || ' '
  ), '%')
  OR PART.P_NAME LIKE CONCAT((
    NVL("_s0".COLOR, '') || ' '
  ), '%')
GROUP BY
  "_s0".COLOR
ORDER BY
  1 NULLS FIRST
