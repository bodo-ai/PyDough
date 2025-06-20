SELECT
  _s0.sbtickerid AS _id
FROM main.sbticker AS _s0
JOIN main.sbdailyprice AS _s1
  ON _s0.sbtickerid = _s1.sbdptickerid AND _s1.sbdpdate >= CAST('2023-04-01' AS DATE)
