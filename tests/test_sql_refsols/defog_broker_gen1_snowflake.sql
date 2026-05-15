SELECT
  MIN(sbdailyprice.sbdpclose) AS lowest_price
FROM broker.sbdailyprice AS sbdailyprice
JOIN broker.sbticker AS sbticker
  ON sbdailyprice.sbdptickerid = sbticker.sbtickerid
  AND sbticker.sbtickersymbol = 'VTI'
WHERE
  DATEDIFF(
    DAY,
    CAST(sbdailyprice.sbdpdate AS DATETIME),
    CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)
  ) <= 7
