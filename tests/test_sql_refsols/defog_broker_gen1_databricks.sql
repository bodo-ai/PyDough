SELECT
  MIN(sbdailyprice.sbdpclose) AS lowest_price
FROM defog.broker.sbdailyprice AS sbdailyprice
JOIN defog.broker.sbticker AS sbticker
  ON sbdailyprice.sbdptickerid = sbticker.sbtickerid
  AND sbticker.sbtickersymbol = 'VTI'
WHERE
  DATEDIFF(DAY, CAST(sbdailyprice.sbdpdate AS DATE), CAST(CURRENT_TIMESTAMP() AS DATE)) <= 7
