SELECT
  identname AS color
FROM clrs
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM shpmnts
    WHERE
      clrs.identname = colid AND dos >= CAST('2025-09-15' AS DATE)
  )
ORDER BY
  1 NULLS FIRST
