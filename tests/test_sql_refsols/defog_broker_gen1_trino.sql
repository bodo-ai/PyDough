SELECT
  MIN(sbdailyprice.sbdpclose) AS lowest_price
FROM cassandra.defog.sbdailyprice AS sbdailyprice
JOIN mysql.broker.sbticker AS sbticker
  ON sbdailyprice.sbdptickerid = sbticker.sbtickerid
  AND sbticker.sbtickersymbol = 'VTI'
WHERE
  DATE_DIFF(
    'DAY',
    CAST(DATE_TRUNC('DAY', CAST(sbdailyprice.sbdpdate AS TIMESTAMP)) AS TIMESTAMP),
    CAST(DATE_TRUNC('DAY', CURRENT_TIMESTAMP) AS TIMESTAMP)
  ) <= 7
