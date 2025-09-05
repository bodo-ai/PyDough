SELECT
  sbticker.sbtickertype AS ticker_type,
  AVG(sbdailyprice.sbdpclose) AS ACP
FROM main.sbticker AS sbticker
JOIN main.sbdailyprice AS sbdailyprice
  ON (
    CAST(CURRENT_TIMESTAMP AS DATE) - CAST(sbdailyprice.sbdpdate AS DATE)
  ) <= 7
  AND sbdailyprice.sbdptickerid = sbticker.sbtickerid
GROUP BY
  1
