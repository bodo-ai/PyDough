WITH _u_0 AS (
  SELECT
    sbdailyprice.sbdptickerid AS _u_1
  FROM main.sbdailyprice AS sbdailyprice
  WHERE
    sbdailyprice.sbdpdate >= '2023-04-01'
  GROUP BY
    sbdailyprice.sbdptickerid
)
SELECT
  sbticker.sbtickerid AS _id
FROM main.sbticker AS sbticker
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbticker.sbtickerid
WHERE
  NOT _u_0._u_1 IS NULL
