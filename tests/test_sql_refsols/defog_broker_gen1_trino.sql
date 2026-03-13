SELECT
  MIN(sbdailyprice.sbdpclose) AS lowest_price
FROM main.sbdailyprice AS sbdailyprice
JOIN main.sbticker AS sbticker
  ON sbdailyprice.sbdptickerid = sbticker.sbtickerid
  AND sbticker.sbtickersymbol = 'VTI'
WHERE
  DATE_DIFF('DAY', CAST(sbdailyprice.sbdpdate AS TIMESTAMP), CURRENT_TIMESTAMP) <= 7
