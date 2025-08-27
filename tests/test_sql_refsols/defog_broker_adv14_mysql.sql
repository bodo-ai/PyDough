WITH _s1 AS (
  SELECT
    COUNT(sbdpclose) AS count_sbDpClose,
    SUM(sbdpclose) AS sum_sbDpClose,
    sbdptickerid AS sbDpTickerId
  FROM main.sbDailyPrice
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), sbdpdate) <= 7
  GROUP BY
    3
)
SELECT
  sbTicker.sbtickertype AS ticker_type,
  SUM(_s1.sum_sbDpClose) / SUM(_s1.count_sbDpClose) AS ACP
FROM main.sbTicker AS sbTicker
JOIN _s1 AS _s1
  ON _s1.sbDpTickerId = sbTicker.sbtickerid
GROUP BY
  1
