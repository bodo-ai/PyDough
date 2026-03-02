WITH _u_0 AS (
  SELECT
    colid AS _u_1
  FROM shpmnts
  WHERE
    dos = CAST('2024-07-04' AS DATE)
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n
FROM clrs AS clrs
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = clrs.identname
WHERE
  NOT _u_0._u_1 IS NULL
