SELECT
  _s0.sbtickerid AS _id
FROM main.sbticker AS _s0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.sbdailyprice AS _s1
    WHERE
      _s0.sbtickerid = _s1.sbdptickerid AND _s1.sbdpdate >= '2023-04-01'
  )
