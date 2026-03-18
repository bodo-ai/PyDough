SELECT
  MIN(sbdailyprice.sbdpclose) AS lowest_price
FROM postgres.main.sbdailyprice AS sbdailyprice
JOIN mysql.broker.sbticker AS sbticker
  ON sbdailyprice.sbdptickerid = sbticker.sbtickerid
  AND sbticker.sbtickersymbol = 'VTI'
WHERE
  DATE_DIFF('DAY', CAST(sbdailyprice.sbdpdate AS TIMESTAMP), CURRENT_TIMESTAMP) <= 7
