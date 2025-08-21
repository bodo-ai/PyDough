SELECT
  sbTicker.sbtickertype AS ticker_type,
  AVG(sbDailyPrice.sbdpclose) AS ACP
FROM main.sbTicker AS sbTicker
JOIN main.sbDailyPrice AS sbDailyPrice
  ON DATEDIFF(CURRENT_TIMESTAMP(), sbDailyPrice.sbdpdate) <= 7
  AND sbDailyPrice.sbdptickerid = sbTicker.sbtickerid
GROUP BY
  1
