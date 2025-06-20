SELECT
  MIN(_s0.sbdpclose) AS lowest_price
FROM main.sbdailyprice AS _s0
JOIN main.sbticker AS _s1
  ON _s0.sbdptickerid = _s1.sbtickerid AND _s1.sbtickersymbol = 'VTI'
WHERE
  DATEDIFF(CURRENT_TIMESTAMP(), _s0.sbdpdate, DAY) <= 7
