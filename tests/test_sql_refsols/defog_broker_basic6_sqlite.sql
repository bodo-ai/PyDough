SELECT
  sbtickerid AS _id
FROM main.sbticker
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.sbdailyprice
    WHERE
      sbdpdate >= '2023-04-01' AND sbdptickerid = sbticker.sbtickerid
  )
