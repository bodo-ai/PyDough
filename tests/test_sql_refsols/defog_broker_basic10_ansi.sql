SELECT
  sbticker.sbtickerid AS _id,
  sbticker.sbtickersymbol AS symbol
FROM main.sbticker AS sbticker
JOIN main.sbdailyprice AS sbdailyprice
  ON sbdailyprice.sbdptickerid = sbticker.sbtickerid
