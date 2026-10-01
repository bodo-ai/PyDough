WITH _s1 AS (
  SELECT
    sbDpTickerId,
    COUNT(sbDpClose) AS count_sbDpClose,
    SUM(sbDpClose) AS sum_sbDpClose
  FROM broker.sbDailyPrice
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), sbDpDate) <= 7
  GROUP BY
    1
)
SELECT
  sbTicker.sbTickerType AS ticker_type,
  SUM(_s1.sum_sbDpClose) / SUM(_s1.count_sbDpClose) AS ACP
FROM broker.sbTicker AS sbTicker
JOIN _s1 AS _s1
  ON _s1.sbDpTickerId = sbTicker.sbTickerId
GROUP BY
  1
