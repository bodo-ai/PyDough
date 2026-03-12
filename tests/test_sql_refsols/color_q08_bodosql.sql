SELECT
  identname AS color
FROM clrs
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM shpmnts
    WHERE
      clrs.identname = colid
  )
ORDER BY
  1 NULLS FIRST
