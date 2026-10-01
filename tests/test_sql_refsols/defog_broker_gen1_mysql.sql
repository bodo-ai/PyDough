SELECT
  MIN(sbDailyPrice.sbDpClose) AS lowest_price
FROM broker.sbDailyPrice AS sbDailyPrice
JOIN broker.sbTicker AS sbTicker
  ON sbDailyPrice.sbDpTickerId = sbTicker.sbTickerId
  AND sbTicker.sbTickerSymbol = 'VTI'
WHERE
  DATEDIFF(CURRENT_TIMESTAMP(), sbDailyPrice.sbDpDate) <= 7
