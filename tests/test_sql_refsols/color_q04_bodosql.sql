SELECT
  COUNT(*) AS n
FROM shpmnts AS shpmnts
JOIN clrs AS clrs
  ON STARTSWITH(clrs.colorname, 'Yellow') AND clrs.identname = shpmnts.colid
WHERE
  YEAR(CAST(shpmnts.dos AS TIMESTAMP)) = 2025
