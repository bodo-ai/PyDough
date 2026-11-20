WITH "_s0" AS (
  SELECT
    P_NAME,
    P_PARTKEY
  FROM TPCH.PART
  ORDER BY
    2 NULLS FIRST
  FETCH FIRST 3 ROWS ONLY
), "_1" AS (
  SELECT DISTINCT
    REGEXP_SUBSTR(P.P_NAME, '[^ ]+', 1, "_0".LVL) AS COLOR
  FROM PART P
  CROSS JOIN LATERAL (
    SELECT
      LEVEL AS LVL
    FROM DUAL
    CONNECT BY LEVEL <= REGEXP_COUNT(P.P_NAME, '[^ ]+')
  )"_0"
)
SELECT
  ANY_VALUE("_s0".P_NAME) AS name,
  MIN("_1".COLOR) AS a,
  MAX("_1".COLOR) AS b
FROM "_s0" "_s0"
JOIN "_1" "_1"
  ON "_s0".P_NAME LIKE CONCAT('%', (
    ' ' || NVL("_1".COLOR, '')
  ))
  OR "_s0".P_NAME LIKE CONCAT('%', (
    ' ' || NVL("_1".COLOR, '') || ' '
  ), '%')
  OR "_s0".P_NAME LIKE CONCAT((
    NVL("_1".COLOR, '') || ' '
  ), '%')
GROUP BY
  "_s0".P_PARTKEY
ORDER BY
  "_s0".P_PARTKEY NULLS FIRST
