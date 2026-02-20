SELECT
  ANY_VALUE(clrs.colorname) AS color,
  ANY_VALUE(clrs.chex) AS hex_code,
  COALESCE(SUM(shpmnts.vol), 0) AS vol_shipped
FROM clrs AS clrs
JOIN shpmnts AS shpmnts
  ON clrs.identname = shpmnts.colid
WHERE
  clrs.r >= (
    clrs.b + 50
  ) AND clrs.r >= (
    clrs.g + 50
  )
GROUP BY
  shpmnts.colid
ORDER BY
  3 DESC NULLS LAST,
  2 NULLS FIRST
LIMIT 3
