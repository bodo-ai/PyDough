WITH _u_0 AS (
  SELECT
    sbdptickerid AS _u_1
  FROM main.sbdailyprice
  WHERE
    sbdpdate >= '2023-04-01'
  GROUP BY
    sbdptickerid
)
SELECT
  sbticker.sbtickerid AS _id
FROM main.sbticker AS sbticker
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbticker.sbtickerid
WHERE
  NOT _u_0._u_1 IS NULL
