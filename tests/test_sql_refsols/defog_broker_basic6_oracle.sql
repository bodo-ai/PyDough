WITH "_u_0" AS (
  SELECT
    sbdptickerid AS "_u_1"
  FROM MAIN.SBDAILYPRICE
  WHERE
    sbdpdate >= TO_DATE('2023-04-01', 'YYYY-MM-DD')
  GROUP BY
    sbdptickerid
)
SELECT
  SBTICKER.sbtickerid AS "_id"
FROM MAIN.SBTICKER SBTICKER
LEFT JOIN "_u_0" "_u_0"
  ON SBTICKER.sbtickerid = "_u_0"."_u_1"
WHERE
  NOT "_u_0"."_u_1" IS NULL
