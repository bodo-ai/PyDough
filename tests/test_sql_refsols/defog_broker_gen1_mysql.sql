SELECT
  MIN(sbDailyPrice.sbdpclose) AS lowest_price
FROM main.sbDailyPrice AS sbDailyPrice
JOIN main.sbTicker AS sbTicker
  ON sbDailyPrice.sbdptickerid = sbTicker.sbtickerid
  AND sbTicker.sbtickersymbol = 'VTI'
WHERE
  DATEDIFF(CURRENT_TIMESTAMP(), CAST(sbDailyPrice.sbdpdate AS DATETIME)) <= 7
