WITH _t1 AS (
  SELECT
    sbtickerid AS _id,
    sbtickersymbol AS symbol
  FROM main.sbticker
), _t4 AS (
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
    _t1.symbol AS symbol
  FROM main.sbdailyprice AS sbdailyprice
  LEFT JOIN _t1 AS _t1
    ON _t1._id = sbdailyprice.sbdptickerid
), _t5 AS (
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
    _t3.symbol AS symbol
  FROM main.sbdailyprice AS sbdailyprice
  LEFT JOIN _t1 AS _t3
    ON _t3._id = sbdailyprice.sbdptickerid
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
    _t3.symbol
)
SELECT
  _t4.symbol AS symbol,
  _t4.month AS month,
  _t5.agg_0 AS avg_close,
  _t5.agg_1 AS max_high,
  _t5.agg_2 AS min_low,
  CAST((
    _t5.agg_0 - LAG(_t5.agg_0, 1) OVER (PARTITION BY _t4.symbol ORDER BY _t4.month)
  ) AS REAL) / LAG(_t5.agg_0, 1) OVER (PARTITION BY _t4.symbol ORDER BY _t4.month) AS momc
FROM _t4 AS _t4
LEFT JOIN _t5 AS _t5
  ON _t4.month = _t5.month AND _t4.symbol = _t5.symbol
