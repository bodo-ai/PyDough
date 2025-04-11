WITH _s1 AS (
  SELECT
    sbtickerid AS _id,
    sbtickersymbol AS symbol
  FROM main.sbticker
), _s4 AS (
  SELECT DISTINCT
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', sbdailyprice.sbdpdate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER), -2)
      END
    ) AS month,
    _s1.symbol
  FROM main.sbdailyprice AS sbdailyprice
  LEFT JOIN _s1 AS _s1
    ON _s1._id = sbdailyprice.sbdptickerid
), _s5 AS (
  SELECT
    AVG(sbdailyprice.sbdpclose) AS agg_0,
    MAX(sbdailyprice.sbdphigh) AS agg_1,
    MIN(sbdailyprice.sbdplow) AS agg_2,
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', sbdailyprice.sbdpdate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER), (
          2 * -1
        ))
      END
    ) AS month,
    _s3.symbol
  FROM main.sbdailyprice AS sbdailyprice
  LEFT JOIN _s1 AS _s3
    ON _s3._id = sbdailyprice.sbdptickerid
  GROUP BY
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', sbdailyprice.sbdpdate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER), (
          2 * -1
        ))
      END
    ),
    _s3.symbol
)
SELECT
  _s4.symbol,
  _s4.month,
  _s5.agg_0 AS avg_close,
  _s5.agg_1 AS max_high,
  _s5.agg_2 AS min_low,
  CAST((
    _s5.agg_0 - LAG(_s5.agg_0, 1) OVER (PARTITION BY _s4.symbol ORDER BY _s4.month)
  ) AS REAL) / LAG(_s5.agg_0, 1) OVER (PARTITION BY _s4.symbol ORDER BY _s4.month) AS momc
FROM _s4 AS _s4
LEFT JOIN _s5 AS _s5
  ON _s4.month = _s5.month AND _s4.symbol = _s5.symbol
