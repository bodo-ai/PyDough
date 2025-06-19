SELECT
  sbticker.sbtickertype AS ticker_type,
  AVG(sbdailyprice.sbdpclose) AS ACP
FROM main.sbticker AS sbticker
JOIN main.sbdailyprice AS sbdailyprice
  ON CAST((
    JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(sbdailyprice.sbdpdate, 'start of day'))
  ) AS INTEGER) <= 7
  AND sbdailyprice.sbdptickerid = sbticker.sbtickerid
GROUP BY
  sbticker.sbtickertype
