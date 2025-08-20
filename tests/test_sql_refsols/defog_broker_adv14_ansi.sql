SELECT
  sbticker.sbtickertype AS ticker_type,
  AVG(sbdailyprice.sbdpclose) AS ACP
FROM main.sbticker AS sbticker
JOIN main.sbdailyprice AS sbdailyprice
  ON DATEDIFF(CURRENT_TIMESTAMP(), CAST(sbdailyprice.sbdpdate AS DATETIME), DAY) <= 7
  AND sbdailyprice.sbdptickerid = sbticker.sbtickerid
GROUP BY
  1
