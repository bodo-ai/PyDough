SELECT
  _s0.sbtickertype AS ticker_type,
  AVG(_s1.sbdpclose) AS ACP
FROM main.sbticker AS _s0
JOIN main.sbdailyprice AS _s1
  ON CAST((
    JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(_s1.sbdpdate, 'start of day'))
  ) AS INTEGER) <= 7
  AND _s0.sbtickerid = _s1.sbdptickerid
GROUP BY
  _s0.sbtickertype
