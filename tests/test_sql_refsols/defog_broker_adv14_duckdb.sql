WITH _s1 AS (
  SELECT
    sbdptickerid,
    COUNT(sbdpclose) AS count_sbdpclose,
    SUM(sbdpclose) AS sum_sbdpclose
  FROM main.sbdailyprice
  WHERE
    DATE_DIFF(
      'DAY',
      CAST(sbdpdate AS TIMESTAMP),
      CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)
    ) <= 7
  GROUP BY
    1
)
SELECT
  sbticker.sbtickertype AS ticker_type,
  SUM(_s1.sum_sbdpclose) / SUM(_s1.count_sbdpclose) AS ACP
FROM main.sbticker AS sbticker
JOIN _s1 AS _s1
  ON _s1.sbdptickerid = sbticker.sbtickerid
GROUP BY
  1
