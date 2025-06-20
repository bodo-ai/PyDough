SELECT
  _s0.sbtickerid AS _id,
  _s0.sbtickersymbol AS symbol
FROM main.sbticker AS _s0
JOIN main.sbdailyprice AS _s1
  ON _s0.sbtickerid = _s1.sbdptickerid
