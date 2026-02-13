SELECT
  MIN(sbdailyprice.sbdpclose) AS lowest_price
FROM main.sbdailyprice AS sbdailyprice
JOIN main.sbticker AS sbticker
  ON sbdailyprice.sbdptickerid = sbticker.sbtickerid
  AND sbticker.sbtickersymbol = 'VTI'
WHERE
  DATEDIFF(
    DAY,
    CAST(sbdailyprice.sbdpdate AS DATETIME),
    CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)
  ) <= 7
