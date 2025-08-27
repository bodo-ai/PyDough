WITH _s1 AS (
  SELECT
    COUNT(sbdpclose) AS count_sbdpclose,
    SUM(sbdpclose) AS sum_sbdpclose,
    sbdptickerid
  FROM main.sbdailyprice
  WHERE
    CAST((
      JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(sbdpdate, 'start of day'))
    ) AS INTEGER) <= 7
  GROUP BY
    3
)
SELECT
  sbticker.sbtickertype AS ticker_type,
  CAST(SUM(_s1.sum_sbdpclose) AS REAL) / SUM(_s1.count_sbdpclose) AS ACP
FROM main.sbticker AS sbticker
JOIN _s1 AS _s1
  ON _s1.sbdptickerid = sbticker.sbtickerid
GROUP BY
  1
