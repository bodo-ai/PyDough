WITH _u_0 AS (
  SELECT
    colid AS _u_1
  FROM shpmnts
  WHERE
    dos >= CAST('2025-09-15' AS DATE)
  GROUP BY
    1
)
SELECT
  clrs.identname AS color
FROM clrs AS clrs
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = clrs.identname
WHERE
  _u_0._u_1 IS NULL
ORDER BY
  1 NULLS FIRST
