WITH _u_0 AS (
  SELECT
    sbdptickerid AS _u_1
  FROM broker.sbdailyprice
  WHERE
    sbdpdate >= CAST('2023-04-01' AS DATE)
  GROUP BY
    1
)
SELECT
  sbticker.sbtickerid AS _id
FROM broker.sbticker AS sbticker
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbticker.sbtickerid
WHERE
  NOT _u_0._u_1 IS NULL
