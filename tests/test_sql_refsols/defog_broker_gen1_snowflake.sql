SELECT
  MIN(sbdailyprice.sbdpclose) AS lowest_price
FROM main.sbdailyprice AS sbdailyprice
JOIN main.sbticker AS sbticker
  ON sbdailyprice.sbdptickerid = sbticker.sbtickerid
  AND sbticker.sbtickersymbol = 'VTI'
WHERE
  DATEDIFF(DAY, CAST(sbdailyprice.sbdpdate AS DATETIME), CURRENT_TIMESTAMP()) <= 7
