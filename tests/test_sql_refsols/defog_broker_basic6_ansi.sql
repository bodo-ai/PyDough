WITH _s1 AS (
  SELECT
    sbdptickerid
  FROM main.sbdailyprice
  WHERE
    sbdpdate >= CAST('2023-04-01' AS DATE)
)
SELECT
  sbticker.sbtickerid AS _id
FROM main.sbticker AS sbticker
SEMI JOIN _s1 AS _s1
  ON _s1.sbdptickerid = sbticker.sbtickerid
