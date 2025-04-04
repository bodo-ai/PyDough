SELECT
  sbticker.sbtickertype AS ticker_type,
  AVG(sbdailyprice.sbdpclose) AS ACP
FROM main.sbdailyprice AS sbdailyprice
LEFT JOIN main.sbticker AS sbticker
  ON sbdailyprice.sbdptickerid = sbticker.sbtickerid
WHERE
  DATEDIFF(CURRENT_TIMESTAMP(), sbdailyprice.sbdpdate, DAY) <= 7
GROUP BY
  sbticker.sbtickertype
