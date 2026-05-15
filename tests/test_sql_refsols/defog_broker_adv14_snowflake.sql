WITH _s1 AS (
  SELECT
    sbdptickerid,
    COUNT(sbdpclose) AS count_sbdpclose,
    SUM(sbdpclose) AS sum_sbdpclose
  FROM broker.sbdailyprice
  WHERE
    DATEDIFF(
      DAY,
      CAST(sbdpdate AS DATETIME),
      CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)
    ) <= 7
  GROUP BY
    1
)
SELECT
  sbticker.sbtickertype AS ticker_type,
  SUM(_s1.sum_sbdpclose) / SUM(_s1.count_sbdpclose) AS ACP
FROM broker.sbticker AS sbticker
JOIN _s1 AS _s1
  ON _s1.sbdptickerid = sbticker.sbtickerid
GROUP BY
  1
