WITH _s1 AS (
  SELECT
    sbdptickerid AS sbDpTickerId,
    COUNT(sbdpclose) AS count_sbDpClose,
    SUM(sbdpclose) AS sum_sbDpClose
  FROM broker.sbDailyPrice
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), sbdpdate) <= 7
  GROUP BY
    1
)
SELECT
  sbTicker.sbtickertype AS ticker_type,
  SUM(_s1.sum_sbDpClose) / SUM(_s1.count_sbDpClose) AS ACP
FROM broker.sbTicker AS sbTicker
JOIN _s1 AS _s1
  ON _s1.sbDpTickerId = sbTicker.sbtickerid
GROUP BY
  1
