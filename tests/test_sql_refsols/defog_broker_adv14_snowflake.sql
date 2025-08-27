SELECT
  sbticker.sbtickertype AS ticker_type,
  AVG(sbdailyprice.sbdpclose) AS ACP
FROM main.sbticker AS sbticker
JOIN main.sbdailyprice AS sbdailyprice
  ON DATEDIFF(DAY, CAST(sbdailyprice.sbdpdate AS DATETIME), CURRENT_TIMESTAMP()) <= 7
  AND sbdailyprice.sbdptickerid = sbticker.sbtickerid
GROUP BY
  1
