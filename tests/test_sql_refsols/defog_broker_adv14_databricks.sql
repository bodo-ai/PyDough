WITH _s1 AS (
  SELECT
    sbdptickerid,
    COUNT(sbdpclose) AS count_sbdpclose,
    SUM(sbdpclose) AS sum_sbdpclose
  FROM defog.broker.sbdailyprice
  WHERE
    DATEDIFF(DAY, CAST(sbdpdate AS DATE), CAST(CURRENT_TIMESTAMP() AS DATE)) <= 7
  GROUP BY
    1
)
SELECT
  sbticker.sbtickertype AS ticker_type,
  SUM(_s1.sum_sbdpclose) / SUM(_s1.count_sbdpclose) AS ACP
FROM defog.broker.sbticker AS sbticker
JOIN _s1 AS _s1
  ON _s1.sbdptickerid = sbticker.sbtickerid
GROUP BY
  1
