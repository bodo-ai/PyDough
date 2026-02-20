WITH _t2 AS (
  SELECT
    shpmnts.colid,
    COUNT(*) AS n_rows,
    SUM(shpmnts.vol) AS sum_vol
  FROM shpmnts AS shpmnts
  JOIN cust AS cust
    ON cust.cstfname = 'Alice' AND cust.cstid = shpmnts.cusid AND cust.cstlname = 'Smith'
  JOIN supls AS supls
    ON shpmnts.comid = supls.supid AND supls.supname = 'Pallette Emporium'
  WHERE
    MONTH(CAST(shpmnts.dos AS TIMESTAMP)) = 12
    AND YEAR(CAST(shpmnts.dos AS TIMESTAMP)) = 2024
  GROUP BY
    1
)
SELECT
  clrs.colorname AS color_name,
  COALESCE(_t2.sum_vol, 0) AS total_volume
FROM clrs AS clrs
JOIN _t2 AS _t2
  ON _t2.colid = clrs.identname AND _t2.n_rows > 1
WHERE
  NOT CONTAINS(clrs.identname, '_')
