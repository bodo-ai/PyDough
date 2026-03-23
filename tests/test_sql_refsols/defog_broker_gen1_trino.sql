SELECT
  MIN(sbdailyprice.sbdpclose) AS lowest_price
FROM postgres.main.sbdailyprice AS sbdailyprice
JOIN mysql.broker.sbticker AS sbticker
  ON sbdailyprice.sbdptickerid = sbticker.sbtickerid
  AND sbticker.sbtickersymbol = 'VTI'
WHERE
  DATE_DIFF(
    'DAY',
    CAST(DATE_TRUNC('DAY', sbdailyprice.sbdpdate) AS TIMESTAMP),
    CAST(DATE_TRUNC('DAY', CURRENT_TIMESTAMP) AS TIMESTAMP)
  ) <= 7
