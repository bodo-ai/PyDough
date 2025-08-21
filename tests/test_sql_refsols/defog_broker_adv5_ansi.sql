WITH _t1 AS (
  SELECT
    COUNT(sbdailyprice.sbdpclose) AS count_sbdpclose,
    MAX(sbdailyprice.sbdphigh) AS max_high,
    MIN(sbdailyprice.sbdplow) AS min_low,
    ANY_VALUE(sbticker.sbtickersymbol) AS sbtickersymbol,
    SUM(sbdailyprice.sbdpclose) AS sum_sbdpclose,
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbdailyprice.sbdpdate AS DATETIME)),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM CAST(sbdailyprice.sbdpdate AS DATETIME))) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM CAST(sbdailyprice.sbdpdate AS DATETIME)), 1, 2)
        ELSE SUBSTRING(
          CONCAT('00', EXTRACT(MONTH FROM CAST(sbdailyprice.sbdpdate AS DATETIME))),
          (
            2 * -1
          )
        )
      END
    ) AS month
  FROM main.sbdailyprice AS sbdailyprice
  JOIN main.sbticker AS sbticker
    ON sbdailyprice.sbdptickerid = sbticker.sbtickerid
  GROUP BY
    sbdailyprice.sbdptickerid,
    6
), _t0 AS (
  SELECT
    MAX(max_high) AS max_high,
    MIN(min_low) AS min_low,
    SUM(count_sbdpclose) AS sum_count_sbdpclose,
    SUM(sum_sbdpclose) AS sum_sum_sbdpclose,
    month,
    sbtickersymbol
  FROM _t1
  GROUP BY
    5,
    6
)
SELECT
  sbtickersymbol AS symbol,
  month,
  sum_sum_sbdpclose / sum_count_sbdpclose AS avg_close,
  max_high,
  min_low,
  (
    (
      sum_sum_sbdpclose / sum_count_sbdpclose
    ) - LAG(sum_sum_sbdpclose / sum_count_sbdpclose, 1) OVER (PARTITION BY sbtickersymbol ORDER BY month NULLS LAST)
  ) / LAG(sum_sum_sbdpclose / sum_count_sbdpclose, 1) OVER (PARTITION BY sbtickersymbol ORDER BY month NULLS LAST) AS momc
FROM _t0
