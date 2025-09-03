SELECT
  sbticker.sbtickertype AS ticker_type,
  AVG(sbdailyprice.sbdpclose) AS ACP
FROM main.sbticker AS sbticker
JOIN main.sbdailyprice AS sbdailyprice
  ON EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - CAST(sbdailyprice.sbdpdate AS TIMESTAMP)) / 86400 <= 7
  AND sbdailyprice.sbdptickerid = sbticker.sbtickerid
GROUP BY
  1
