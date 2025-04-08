SELECT
  sbticker.sbtickertype AS ticker_type,
  AVG(sbdailyprice.sbdpclose) AS ACP
FROM main.sbdailyprice AS sbdailyprice
LEFT JOIN main.sbticker AS sbticker
  ON sbdailyprice.sbdptickerid = sbticker.sbtickerid
WHERE
  CAST((
    JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(sbdailyprice.sbdpdate, 'start of day'))
  ) AS INTEGER) <= 7
GROUP BY
  sbticker.sbtickertype
