SELECT
  sbticker.sbtickerid AS _id
FROM main.sbticker AS sbticker
JOIN main.sbdailyprice AS sbdailyprice
  ON sbdailyprice.sbdpdate >= CAST('2023-04-01' AS DATE)
  AND sbdailyprice.sbdptickerid = sbticker.sbtickerid
