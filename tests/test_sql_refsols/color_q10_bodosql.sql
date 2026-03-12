SELECT
  COUNT(*) AS n
FROM clrs
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM shpmnts
    WHERE
      clrs.identname = colid AND dos = CAST('2024-07-04' AS DATE)
  )
